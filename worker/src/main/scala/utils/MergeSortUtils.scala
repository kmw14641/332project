package utils

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import scala.jdk.CollectionConverters._
import java.util.concurrent.{Executors, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration._
import common.utils.SystemUtils
import com.google.protobuf.ByteString
import scala.annotation.tailrec

object MergeSortUtils {
  val RECORD_SIZE = 100  // 10 bytes key + 90 bytes value
  val KEY_SIZE = 10
  val VALUE_SIZE = 90
  val MAX_CHUNK_SIZE_MB = 32  // Upper bound: 32 MiB
  
  /**
   * Calculate chunk size: at least 3 files should fit in RAM, with upper bound of 32MiB
   */
  def getChunkSize: Long = {
    val ramBytes = SystemUtils.getRamMb * 1024 * 1024
    val maxChunkBytes = MAX_CHUNK_SIZE_MB * 1024 * 1024
    // Ensure at least 3 chunks can fit in RAM (use 80% of RAM)
    val chunkBytesForRam = (ramBytes * 0.8 / 3).toLong
    val chunkBytes = Math.min(maxChunkBytes, chunkBytesForRam)
    chunkBytes / RECORD_SIZE // At least 1000 records
  }
  
  /**
   * Calculate how many files can be loaded in RAM simultaneously
   * Used to determine thread pool size
   */
  def getMaxConcurrentFiles: Int = {
    val ramBytes = SystemUtils.getRamMb * 1024 * 1024
    val chunkBytes = getChunkSize * RECORD_SIZE
    // Use 80% of RAM, and we need 3 files per merge operation (2 input + 1 output)
    val maxFiles = ((ramBytes * 0.8) / chunkBytes).toInt
    Math.max(3, maxFiles) // At least 3 files
  }
  
  /**
   * Calculate optimal thread count based on CPU cores and RAM constraints
   */
  def getThreadCount: Int = {
    val cpuThreads = SystemUtils.getProcessorNum * 2
    val maxConcurrentFiles = getMaxConcurrentFiles
    // Each merge task needs 3 files (2 input + 1 output)
    val ramBasedThreads = maxConcurrentFiles / 3
    Math.min(cpuThreads, ramBasedThreads)
  }

  /**
   * Disk-based external merge sort (2-way merge)
   * 
   * @param inputDirs Input directories containing unsorted files
   * @param intermediateDirPath Directory to store intermediate sorted chunks and merge results
   */
  def diskBasedMergeSort(inputDirs: Seq[String], intermediateDirPath: String): List[String] = {
    println(s"[MergeSort] Starting disk-based merge sort with ${inputDirs.size} input directories")
    
    // Create intermediate directory
    PathUtils.createDirectoryIfNotExists(intermediateDirPath)
    
    val chunkSize = getChunkSize
    println(s"[MergeSort] Chunk size: $chunkSize records (${chunkSize * RECORD_SIZE / (1024 * 1024)} MB)")
    
    // Phase 1: In-memory sort and write to intermediate files
    val sortedFiles = inMemorySort(inputDirs, intermediateDirPath, chunkSize)
    println(s"[MergeSort] Phase 1 complete: Created ${sortedFiles.size} sorted files")
    
    // Phase 2: 2-way merge until one file remains
    val finalOutputs = twoWayMergeSort(sortedFiles, intermediateDirPath)
    println(s"[MergeSort] Phase 2 complete: ${finalOutputs.size} sorted files in order")

    finalOutputs
  }

  /**
   * Phase 1: In-memory sort of all input files
   * Split files into chunks if they exceed chunk size
   */
  private def inMemorySort(inputDirs: Seq[String], sortDir: String, chunkSize: Long): List[String] = {
    // Collect all input files
    val allFiles = inputDirs.flatMap { dirPath =>
      PathUtils.getFilesList(dirPath).filter { filePath =>
        Files.isRegularFile(Paths.get(filePath)) && Files.size(Paths.get(filePath)) >= RECORD_SIZE
      }
    }
    
    println(s"[MergeSort] Found ${allFiles.size} input files to sort")
    
    val threadCount = getThreadCount
    println(s"[MergeSort] Using $threadCount threads for sorting (max concurrent files: ${getMaxConcurrentFiles})")
    
    val sortedFiles = new ConcurrentLinkedQueue[String]()
    val threadPool = Executors.newFixedThreadPool(threadCount)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)
    
    var fileId = 0
    val futures = allFiles.map { filePath =>
      Future {
        val threadId = Thread.currentThread().getName
        val file = Paths.get(filePath)
        val fileSize = Files.size(file)
        val numRecords = fileSize / RECORD_SIZE
        val comparator = ByteString.unsignedLexicographicalComparator
        
        println(s"[MergeSort-InMemory][$threadId] Processing file: $filePath (${numRecords} records)")
        
        // Process chunks recursively
        @tailrec
        def processChunks(offset: Long, chunkCount: Int): Unit = {
          if (offset < numRecords) {
            val recordsToRead = Math.min(chunkSize, numRecords - offset).toInt
            val currentChunk = chunkCount + 1
            
            println(s"[MergeSort-InMemory][$threadId] Reading chunk $currentChunk: $recordsToRead records from offset $offset")
            val records = readRecords(filePath, offset, recordsToRead)
            
            println(s"[MergeSort-InMemory][$threadId] Sorting chunk $currentChunk...")
            val sortedRecords = records.sortWith((a, b) => comparator.compare(a._1, b._1) < 0)
            
            val outputPath = synchronized {
              fileId += 1
              s"$sortDir/$fileId.bin"
            }
            println(s"[MergeSort-InMemory][$threadId] Writing sorted chunk to: $outputPath")
            writeRecords(outputPath, sortedRecords)
            sortedFiles.add(outputPath)
            
            processChunks(offset + recordsToRead, currentChunk)
          } else {
            println(s"[MergeSort-InMemory][$threadId] ✓ Completed file: $filePath ($chunkCount chunks)")
          }
        }
        
        processChunks(0L, 0)
      }
    }
    
    try {
      Await.result(Future.sequence(futures), Duration.Inf)
    } finally {
      threadPool.shutdown()
    }
    
    sortedFiles.asScala.toList.sortBy { path =>
      val name = Paths.get(path).getFileName.toString
      name.stripSuffix(".bin").toInt
    }
  }

  /**
   * Phase 2: 2-way merge sort
   * Merge pairs of file lists until all files are sorted in order
   * 
   * Example with 4 initial files:
   * Round 0: [[1],[2],[3],[4]]
   * Round 1: merge [1] and [3] -> [a,b], merge [2] and [4] -> [c,d]
   *          Result: [[a,b], [c,d]]
   * Round 2: merge [a,b] and [c,d] sequentially:
   *          - Start with files a and c
   *          - If a is exhausted, continue with b and c
   *          - If c is exhausted, continue with a (or b) and d
   *          - Continue until all files are merged
   *          Result: [[e,f,g,h]]
   * After this round, files in [e,f,g,h] are guaranteed to be sorted in order
   */
  private def twoWayMergeSort(files: List[String], sortDir: String): List[String] = {
    // Initialize: each file is its own list
    var fileLists: List[List[String]] = files.map(f => List(f))
    val nextFileId = new AtomicInteger(files.size + 1)
    
    // Calculate number of merge rounds needed
    val totalRounds = Math.ceil(Math.log(fileLists.size) / Math.log(2)).toInt
    println(s"[MergeSort] Total merge rounds needed: $totalRounds")
    
    for (round <- 1 to totalRounds) {
      println(s"[MergeSort] Merge round $round/$totalRounds: ${fileLists.size} file lists, total ${fileLists.map(_.size).sum} files")
      
      val threadCount = getThreadCount
      val threadPool = Executors.newFixedThreadPool(threadCount)
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)
      
      // Pair up file lists
      // [[1],[2],[3],[4]] -> [([1],[3]), ([2],[4])]
      val firstHalf = fileLists.take(fileLists.size / 2)
      val secondHalf = fileLists.drop(fileLists.size / 2)
      val listPairs = firstHalf.zip(secondHalf)
      
      // Handle remaining list if odd number
      val remainingList = if (fileLists.size % 2 == 1) {
        Some(fileLists.last)
      } else {
        None
      }
      
      // For each pair of lists, merge all files sequentially
      val nextFileLists = new ConcurrentLinkedQueue[List[String]]()
      
      val futures = listPairs.toList.map { case (list1, list2) =>
        Future {
          val threadId = Thread.currentThread().getName
          println(s"[MergeSort-Round$round][$threadId] Merging list pair: ${list1.size} files + ${list2.size} files")
          
          // Calculate average chunk size for output files
          val totalSize = (list1 ++ list2).map(f => Files.size(Paths.get(f))).sum
          val totalFiles = list1.size + list2.size
          val avgChunkSize = (totalSize / totalFiles / RECORD_SIZE).toInt
          
          // Pre-allocate enough file IDs for this merge (estimate: total files)
          val estimatedOutputFiles = Math.max(1, totalFiles)
          val startFileId = nextFileId.getAndAdd(estimatedOutputFiles)
          
          val outputFiles = mergeFileLists(list1, list2, sortDir, startFileId, avgChunkSize, threadId, round)
          
          nextFileLists.add(outputFiles)
          println(s"[MergeSort-Round$round][$threadId] Completed merge: ${outputFiles.size} output files")
        }
      }
      
      try {
        Await.result(Future.sequence(futures), Duration.Inf)
      } finally {
        threadPool.shutdown()
      }
      
      // Add remaining list if exists
      remainingList.foreach { list =>
        // Rename files in the remaining list
        val startId = nextFileId.getAndAdd(list.size)
        val renamedFiles = list.zipWithIndex.map { case (file, idx) =>
          val outputPath = s"$sortDir/${startId + idx}.bin"
          Files.move(Paths.get(file), Paths.get(outputPath), 
            java.nio.file.StandardCopyOption.REPLACE_EXISTING)
          outputPath
        }
        nextFileLists.add(renamedFiles)
        println(s"[MergeSort] Renamed ${list.size} remaining files")
      }
      
      fileLists = nextFileLists.asScala.toList
    }
    
    // Flatten all lists to return single sorted list
    fileLists.flatten
  }

  /**
   * Merge two lists of sorted files sequentially
   * When one file is exhausted, continue with the next file from that list
   */
  private def mergeFileLists(
    list1: List[String], 
    list2: List[String], 
    sortDir: String, 
    startFileId: Int,
    avgChunkSize: Int,
    threadId: String,
    round: Int
  ): List[String] = {
    val comparator = ByteString.unsignedLexicographicalComparator
    val outputFiles = scala.collection.mutable.ListBuffer[String]()
    
    var fileId = startFileId
    var idx1 = 0  // Current index in list1
    var idx2 = 0  // Current index in list2
    
    var reader1: Option[FileReader] = None
    var reader2: Option[FileReader] = None
    var writer: Option[FileWriter] = None
    
    var current1: Option[(ByteString, ByteString)] = None
    var current2: Option[(ByteString, ByteString)] = None
    
    var recordsWritten = 0
    
    // Helper function to open next file from list1
    def openNextFile1(): Unit = {
      reader1.foreach(_.close())
      if (idx1 < list1.size) {
        println(s"[MergeSort-Round$round][$threadId] Opening file from list1: ${list1(idx1)} (${idx1 + 1}/${list1.size})")
        reader1 = Some(new FileReader(list1(idx1)))
        current1 = reader1.get.readNext()
        idx1 += 1
      } else {
        reader1 = None
        current1 = None
      }
    }
    
    // Helper function to open next file from list2
    def openNextFile2(): Unit = {
      reader2.foreach(_.close())
      if (idx2 < list2.size) {
        println(s"[MergeSort-Round$round][$threadId] Opening file from list2: ${list2(idx2)} (${idx2 + 1}/${list2.size})")
        reader2 = Some(new FileReader(list2(idx2)))
        current2 = reader2.get.readNext()
        idx2 += 1
      } else {
        reader2 = None
        current2 = None
      }
    }
    
    // Helper function to create new output file
    def createNewOutputFile(): Unit = {
      writer.foreach(_.close())
      val outputPath = s"$sortDir/$fileId.bin"
      println(s"[MergeSort-Round$round][$threadId] Creating output file: $outputPath")
      writer = Some(new FileWriter(outputPath))
      outputFiles += outputPath
      fileId += 1
      recordsWritten = 0
    }
    
    // Initialize first files and first output
    openNextFile1()
    openNextFile2()
    createNewOutputFile()
    
    // Merge loop
    while (current1.isDefined || current2.isDefined) {
      // Check if we need to create a new output file
      if (recordsWritten >= avgChunkSize && avgChunkSize > 0) {
        createNewOutputFile()
      }
      
      (current1, current2) match {
        case (Some((key1, value1)), Some((key2, value2))) =>
          // Both files have data, compare and write smaller
          if (comparator.compare(key1, key2) <= 0) {
            writer.get.write(key1, value1)
            recordsWritten += 1
            current1 = reader1.get.readNext()
            if (current1.isEmpty) openNextFile1()
          } else {
            writer.get.write(key2, value2)
            recordsWritten += 1
            current2 = reader2.get.readNext()
            if (current2.isEmpty) openNextFile2()
          }
        
        case (Some((key1, value1)), None) =>
          // Only list1 has data
          writer.get.write(key1, value1)
          recordsWritten += 1
          current1 = reader1.get.readNext()
          if (current1.isEmpty) openNextFile1()
        
        case (None, Some((key2, value2))) =>
          // Only list2 has data
          writer.get.write(key2, value2)
          recordsWritten += 1
          current2 = reader2.get.readNext()
          if (current2.isEmpty) openNextFile2()
        
        case (None, None) =>
          // Both exhausted, should not happen
          throw new RuntimeException(s"[MergeSort-Round$round][$threadId] Error: Both file readers exhausted unexpectedly")
      }
    }
    
    // Clean up
    reader1.foreach(_.close())
    reader2.foreach(_.close())
    writer.foreach(_.close())
    
    // Delete input files that were merged
    (list1 ++ list2).foreach { file =>
      try {
        Files.deleteIfExists(Paths.get(file))
        println(s"[MergeSort-Round$round][$threadId] Deleted intermediate file: $file")
      } catch {
        case e: Exception => println(s"[MergeSort-Round$round][$threadId] Warning: Failed to delete $file: ${e.getMessage}")
      }
    }
    
    outputFiles.toList
  }

  /**
   * FileReader class to manage channel and buffer efficiently
   */
  private class FileReader(val path: String) {
    private val channel = FileChannel.open(Paths.get(path), StandardOpenOption.READ)
    private val fileSize = Files.size(Paths.get(path))
    private var position = 0L
    private val keyBuffer = ByteBuffer.allocate(KEY_SIZE)
    private val valueBuffer = ByteBuffer.allocate(VALUE_SIZE)
    
    def hasMore: Boolean = position < fileSize
    
    def readNext(): Option[(ByteString, ByteString)] = {
      if (hasMore) {
        keyBuffer.clear()
        valueBuffer.clear()
        
        val keyBytesRead = channel.read(keyBuffer, position)
        val valueBytesRead = channel.read(valueBuffer, position + KEY_SIZE)
        
        if (keyBytesRead != KEY_SIZE || valueBytesRead != VALUE_SIZE) {
          println(s"[FileReader] Warning: Incomplete read at position $position in $path (key: $keyBytesRead/$KEY_SIZE, value: $valueBytesRead/$VALUE_SIZE)")
          None
        } else {
          position += RECORD_SIZE
          
          // Flip buffers to read from beginning
          keyBuffer.flip()
          valueBuffer.flip()
          
          val key = ByteString.copyFrom(keyBuffer)
          val value = ByteString.copyFrom(valueBuffer)
          
          Some((key, value))
        }
      } else {
        None
      }
    }
    
    def close(): Unit = channel.close()
  }
  
  /**
   * FileWriter class to manage output channel and buffer efficiently
   */
  private class FileWriter(val path: String) {
    private val channel = FileChannel.open(
      Paths.get(path),
      StandardOpenOption.CREATE,
      StandardOpenOption.WRITE,
      StandardOpenOption.TRUNCATE_EXISTING
    )
    private val keyBuffer = ByteBuffer.allocate(KEY_SIZE)
    private val valueBuffer = ByteBuffer.allocate(VALUE_SIZE)
    
    def write(key: ByteString, value: ByteString): Unit = {
      keyBuffer.clear()
      keyBuffer.put(key.toByteArray)
      keyBuffer.flip()
      while (keyBuffer.hasRemaining) {
        channel.write(keyBuffer)
      }
      
      valueBuffer.clear()
      valueBuffer.put(value.toByteArray)
      valueBuffer.flip()
      while (valueBuffer.hasRemaining) {
        channel.write(valueBuffer)
      }
    }
    
    def close(): Unit = channel.close()
  }

  /**
   * Read records from file
   */
  private def readRecords(filePath: String, offset: Long, count: Int): Array[(ByteString, ByteString)] = {
    val file = Paths.get(filePath)
    val channel = FileChannel.open(file, StandardOpenOption.READ)
    
    try {
      val records = Array.ofDim[(ByteString, ByteString)](count)
      val keyBuffer = ByteBuffer.allocate(KEY_SIZE)
      val valueBuffer = ByteBuffer.allocate(VALUE_SIZE)
      
      var position = offset * RECORD_SIZE
      var i = 0
      while (i < count) {
        keyBuffer.clear()
        valueBuffer.clear()
        
        val keyBytesRead = channel.read(keyBuffer, position)
        val valueBytesRead = channel.read(valueBuffer, position + KEY_SIZE)
        
        if (keyBytesRead != KEY_SIZE || valueBytesRead != VALUE_SIZE) {
          throw new RuntimeException(s"Incomplete read at position $position in $filePath (key: $keyBytesRead/$KEY_SIZE, value: $valueBytesRead/$VALUE_SIZE)")
        }
        
        // Flip buffers to read from beginning
        keyBuffer.flip()
        valueBuffer.flip()
        
        records(i) = (ByteString.copyFrom(keyBuffer), ByteString.copyFrom(valueBuffer))
        position += RECORD_SIZE
        i += 1
      }
      
      records
    } finally {
      channel.close()
    }
  }

  /**
   * Write records to file
   */
  private def writeRecords(filePath: String, records: Array[(ByteString, ByteString)]): Unit = {
    val channel = FileChannel.open(
      Paths.get(filePath),
      StandardOpenOption.CREATE,
      StandardOpenOption.WRITE,
      StandardOpenOption.TRUNCATE_EXISTING
    )
    
    try {
      val keyBuffer = ByteBuffer.allocate(KEY_SIZE)
      val valueBuffer = ByteBuffer.allocate(VALUE_SIZE)
      
      var i = 0
      while (i < records.length) {
        val (key, value) = records(i)
        
        keyBuffer.clear()
        keyBuffer.put(key.toByteArray)
        keyBuffer.flip()
        while (keyBuffer.hasRemaining) {
          channel.write(keyBuffer)
        }
        
        valueBuffer.clear()
        valueBuffer.put(value.toByteArray)
        valueBuffer.flip()
        while (valueBuffer.hasRemaining) {
          channel.write(valueBuffer)
        }
        
        i += 1
      }
    } finally {
      channel.close()
    }
  }
}
