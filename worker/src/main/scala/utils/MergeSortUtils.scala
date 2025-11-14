package utils

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import scala.jdk.CollectionConverters._
import java.util.concurrent.{Executors, ConcurrentLinkedQueue}
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration._
import common.utils.SystemUtils
import com.google.protobuf.ByteString

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
    val sortedDir = Paths.get(intermediateDirPath, "sorted")
    PathUtils.createDirectoryIfNotExists(sortedDir.toString)
    
    val chunkSize = getChunkSize
    println(s"[MergeSort] Chunk size: $chunkSize records (${chunkSize * RECORD_SIZE / (1024 * 1024)} MB)")
    
    // Phase 1: In-memory sort and write to intermediate files
    val sortedFiles = inMemorySort(inputDirs, sortedDir.toString, chunkSize)
    println(s"[MergeSort] Phase 1 complete: Created ${sortedFiles.size} sorted files")
    
    // Phase 2: 2-way merge until one file remains
    val finalOutputs = twoWayMergeSort(sortedFiles, sortedDir.toString)
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
        
        var offset = 0L
        var chunkCount = 0
        while (offset < numRecords) {
          chunkCount += 1
          val recordsToRead = Math.min(chunkSize, numRecords - offset).toInt
          println(s"[MergeSort-InMemory][$threadId] Reading chunk $chunkCount: $recordsToRead records from offset $offset")
          
          val records = readRecords(filePath, offset, recordsToRead)
          
          // Sort records in memory by key
          println(s"[MergeSort-InMemory][$threadId] Sorting chunk $chunkCount...")
          val sortedRecords = records.sortWith((a, b) => comparator.compare(a._1, b._1) < 0)
          
          // Write sorted chunk to file
          val outputPath = synchronized {
            fileId += 1
            s"$sortDir/$fileId.bin"
          }
          println(s"[MergeSort-InMemory][$threadId] Writing sorted chunk to: $outputPath")
          writeRecords(outputPath, sortedRecords)
          sortedFiles.add(outputPath)
          
          offset += recordsToRead
        }
        
        println(s"[MergeSort-InMemory][$threadId] ✓ Completed file: $filePath ($chunkCount chunks)")
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
   * Round 2: merge [a,b] and [c,d] at index 0 -> [e,f], at index 1 -> [g,h]
   *          Result: [[e,f,g,h]]
   * After this round, files in [e,f,g,h] are guaranteed to be sorted in order
   */
  private def twoWayMergeSort(files: List[String], sortDir: String): List[String] = {
    // Initialize: each file is its own list
    var fileLists: List[List[String]] = files.map(f => List(f))
    var nextFileId = files.size + 1
    
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
      
      // For each pair of lists, merge files at same indices
      val nextFileLists = new ConcurrentLinkedQueue[List[String]]()
      
      val futures = listPairs.flatMap { case (list1, list2) =>
        // Determine how many merge operations needed
        val maxSize = Math.max(list1.size, list2.size)
        val mergedFilesQueue = new ConcurrentLinkedQueue[String]()
        
        // Create a future for each individual merge operation
        val mergeFutures = (0 until maxSize).map { i =>
          Future {
            val threadId = Thread.currentThread().getName
            val file1Opt = list1.lift(i)
            val file2Opt = list2.lift(i)
            
            val outputs = (file1Opt, file2Opt) match {
              case (Some(file1), Some(file2)) =>
                // Merge two files into two output files
                val (output1Path, output2Path) = synchronized {
                  val id1 = nextFileId
                  val id2 = nextFileId + 1
                  nextFileId += 2
                  (s"$sortDir/$id1.bin", s"$sortDir/$id2.bin")
                }
                
                val file1Size = Files.size(Paths.get(file1))
                val file2Size = Files.size(Paths.get(file2))
                println(s"[MergeSort-Merge][$threadId] Merging index $i: $file1 (${file1Size/RECORD_SIZE} rec) + $file2 (${file2Size/RECORD_SIZE} rec) -> $output1Path, $output2Path")
                
                mergeTwoFiles(file1, file2, output1Path, output2Path)
                
                // Delete input files after merge
                Files.deleteIfExists(Paths.get(file1))
                Files.deleteIfExists(Paths.get(file2))
                
                println(s"[MergeSort-Merge][$threadId] ✓ Completed merge index $i")
                List(output1Path, output2Path)
                
              case (Some(file1), None) =>
                // Only file from list1, just rename it
                val outputPath = synchronized {
                  val id = nextFileId
                  nextFileId += 1
                  s"$sortDir/$id.bin"
                }
                println(s"[MergeSort-Merge][$threadId] Renaming (no pair) index $i: $file1 -> $outputPath")
                Files.move(Paths.get(file1), Paths.get(outputPath), 
                  java.nio.file.StandardCopyOption.REPLACE_EXISTING)
                println(s"[MergeSort-Merge][$threadId] ✓ Completed rename index $i")
                List(outputPath)
                
              case (None, Some(file2)) =>
                // Only file from list2, just rename it
                val outputPath = synchronized {
                  val id = nextFileId
                  nextFileId += 1
                  s"$sortDir/$id.bin"
                }
                println(s"[MergeSort-Merge][$threadId] Renaming (no pair) index $i: $file2 -> $outputPath")
                Files.move(Paths.get(file2), Paths.get(outputPath), 
                  java.nio.file.StandardCopyOption.REPLACE_EXISTING)
                println(s"[MergeSort-Merge][$threadId] ✓ Completed rename index $i")
                List(outputPath)
                
              case (None, None) =>
                // Should not happen
                throw new IllegalStateException("Both files are None")
            }
            
            (i, outputs)
          }
        }
        
        // Wait for all merges for this pair and combine results
        val pairFuture = Future.sequence(mergeFutures).map { indexedOutputs =>
          // Sort by index to maintain order
          val sortedOutputs = indexedOutputs.sortBy(_._1).flatMap(_._2).toList
          nextFileLists.add(sortedOutputs)
        }
        
        List(pairFuture)
      }
      
      try {
        Await.result(Future.sequence(futures), Duration.Inf)
      } finally {
        threadPool.shutdown()
      }
      
      // Add remaining list if exists
      remainingList.foreach { list =>
        // Rename files in the remaining list
        val renamedFiles = list.map { file =>
          val outputPath = synchronized {
            val id = nextFileId
            nextFileId += 1
            s"$sortDir/$id.bin"
          }
          Files.move(Paths.get(file), Paths.get(outputPath), 
            java.nio.file.StandardCopyOption.REPLACE_EXISTING)
          outputPath
        }
        nextFileLists.add(renamedFiles)
      }
      
      fileLists = nextFileLists.asScala.toList
    }
    
    // Flatten all lists to return single sorted list
    fileLists.flatten
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
          
          // Copy data to new arrays to avoid buffer reuse issues
          val keyArray = new Array[Byte](KEY_SIZE)
          val valueArray = new Array[Byte](VALUE_SIZE)
          keyBuffer.get(keyArray)
          valueBuffer.get(valueArray)
          
          Some((ByteString.copyFrom(keyArray), ByteString.copyFrom(valueArray)))
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
      channel.write(keyBuffer)
      
      valueBuffer.clear()
      valueBuffer.put(value.toByteArray)
      valueBuffer.flip()
      channel.write(valueBuffer)
    }
    
    def close(): Unit = channel.close()
  }

  /**
   * Merge two sorted files into two output files of equal size
   * Since both files are already sorted, we can read from top sequentially
   */
  private def mergeTwoFiles(file1Path: String, file2Path: String, output1Path: String, output2Path: String): Unit = {
    val reader1 = new FileReader(file1Path)
    val reader2 = new FileReader(file2Path)
    val writer1 = new FileWriter(output1Path)
    val writer2 = new FileWriter(output2Path)
    
    try {
      val comparator = ByteString.unsignedLexicographicalComparator
      
      // Calculate total records and split point
      val totalRecords = (Files.size(Paths.get(file1Path)) + Files.size(Paths.get(file2Path))) / RECORD_SIZE
      val recordsPerFile = (totalRecords + 1) / 2  // Split evenly
      
      var record1 = reader1.readNext()
      var record2 = reader2.readNext()
      var recordsWritten = 0L
      var currentWriter = writer1
      
      // Merge two sorted streams
      while (record1.isDefined || record2.isDefined) {
        // Switch to second writer at midpoint
        if (recordsWritten == recordsPerFile) {
          currentWriter = writer2
        }
        
        val (keyToWrite, valueToWrite) = (record1, record2) match {
          case (Some((k1, v1)), Some((k2, v2))) =>
            // Compare keys
            if (comparator.compare(k1, k2) <= 0) {
              record1 = reader1.readNext()
              (k1, v1)
            } else {
              record2 = reader2.readNext()
              (k2, v2)
            }
          case (Some((k1, v1)), None) =>
            record1 = reader1.readNext()
            (k1, v1)
          case (None, Some((k2, v2))) =>
            record2 = reader2.readNext()
            (k2, v2)
          case (None, None) =>
            throw new IllegalStateException("Both records are None")
        }
        
        currentWriter.write(keyToWrite, valueToWrite)
        recordsWritten += 1
      }
    } finally {
      reader1.close()
      reader2.close()
      writer1.close()
      writer2.close()
    }
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
      for (i <- 0 until count) {
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
        
        // Copy data to new arrays
        val keyArray = new Array[Byte](KEY_SIZE)
        val valueArray = new Array[Byte](VALUE_SIZE)
        keyBuffer.get(keyArray)
        valueBuffer.get(valueArray)
        
        records(i) = (ByteString.copyFrom(keyArray), ByteString.copyFrom(valueArray))
        position += RECORD_SIZE
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
      
      records.foreach { case (key, value) =>
        keyBuffer.clear()
        keyBuffer.put(key.toByteArray)
        keyBuffer.flip()
        channel.write(keyBuffer)
        
        valueBuffer.clear()
        valueBuffer.put(value.toByteArray)
        valueBuffer.flip()
        channel.write(valueBuffer)
      }
    } finally {
      channel.close()
    }
  }
}
