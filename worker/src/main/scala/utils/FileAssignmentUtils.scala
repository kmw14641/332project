package utils

import java.nio.file.{Files, Paths, StandardOpenOption, StandardCopyOption}
import java.nio.channels.FileChannel
import java.nio.ByteBuffer
import com.google.protobuf.ByteString
import scala.collection.mutable
import common.utils.SystemUtils

object FileAssignmentUtils {
  val RECORD_SIZE = 100
  val KEY_SIZE = 10
  val VALUE_SIZE = 90
  
  /**
   * Assign sorted files to workers based on assigned ranges
   * 
   * @param sortedFiles List of sorted file paths in order
   * @param assignedRange Map of (workerIp, workerPort) -> (startKey, endKey)
   * @param outputDir Directory to write assigned files
   * @return Map of (workerIp, workerPort) -> List[filePath]
   */
  def assignFilesToWorkers(
    sortedFiles: List[String],
    assignedRange: Map[(String, Int), (ByteString, ByteString)],
    outputDir: String
  ): Map[(String, Int), List[String]] = {
    println(s"[FileAssignment] Starting file assignment with ${sortedFiles.size} sorted files")
    
    // Sort workers by start key
    val comparator = ByteString.unsignedLexicographicalComparator
    val sortedWorkers = assignedRange.toList.sortWith { case ((_, (a, _)), (_, (b, _))) => comparator.compare(a, b) < 0 }
    
    println(s"[FileAssignment] Worker ranges (sorted):")
    sortedWorkers.foreach { case ((ip, port), (start, end)) =>
      println(s"  $ip:$port -> [${new java.math.BigInteger(1, start.toByteArray()).toString(16)}, ${new java.math.BigInteger(1, end.toByteArray()).toString(16)})")
    }
    
    // Create output directory
    PathUtils.createDirectoryIfNotExists(outputDir)
    
    // Get file metadata (filename, startKey, endKey) in sorted order
    val fileMetadata = sortedFiles.map { filePath =>
      val (firstKey, lastKey) = getFirstAndLastKeyFromFile(filePath)
      (filePath, firstKey, lastKey)
    }
    
    // Use deque to process files
    val fileDeque = mutable.Queue[(String, ByteString, ByteString)]()
    fileDeque.enqueueAll(fileMetadata)
    
    // Track file counters for each worker
    val fileCounters = mutable.Map[(String, Int), Int]().withDefaultValue(0)
    val assignedFiles = mutable.Map[(String, Int), List[String]]().withDefaultValue(List.empty)
    
    val from = SystemUtils.getLocalIp.get
    
    // Process each worker in order
    var continueToNextWorker = false
    sortedWorkers.foreach { case ((workerIp, workerPort), (rangeStart, rangeEnd)) =>
      if (continueToNextWorker) return Map.empty

      val to = workerIp
      println(s"[FileAssignment] Processing worker $to")
      
      // Process files for this worker
      var keepProcessing = true
      while (fileDeque.nonEmpty && keepProcessing) {
        val (filePath, fileStartKey, fileEndKey) = fileDeque.dequeue()
        
        println(s"[FileAssignment]   Checking file: $filePath")
        
        // Check if file's end key is within [rangeStart, rangeEnd)
        val fileEndInRange = comparator.compare(fileEndKey, rangeStart) >= 0 && 
                             comparator.compare(fileEndKey, rangeEnd) < 0
        
        if (fileEndInRange) {
          // Entire file belongs to this worker - just rename
          val fileNum = fileCounters((workerIp, workerPort))
          fileCounters((workerIp, workerPort)) = fileNum + 1
          
          val newFileName = s"$from-$to-$fileNum"
          val newFilePath = Paths.get(outputDir, newFileName)
          
          Files.move(Paths.get(filePath), newFilePath, StandardCopyOption.REPLACE_EXISTING)
          assignedFiles((workerIp, workerPort)) = assignedFiles((workerIp, workerPort)) :+ newFilePath.toString
          
          println(s"[FileAssignment]   ✓ Renamed entire file to: $newFileName")
          
        } else {
          // File's end key is beyond this worker's range - need to split
          // Check if rangeEnd falls within this file
          val rangeEndInFile = comparator.compare(rangeEnd, fileStartKey) > 0 && 
                               comparator.compare(rangeEnd, fileEndKey) <= 0
          
          if (rangeEndInFile) {
            // Split file at rangeEnd
            println(s"[FileAssignment]   Splitting file at rangeEnd...")
            
            // Load file into memory and find split point
            val records = loadFileRecords(filePath)
            val splitIndex = findSplitIndex(records, rangeEnd)
            
            // Split into two parts
            val part1Records = records.take(splitIndex)
            val part2Records = records.drop(splitIndex)
            
            // Part 1: belongs to current worker
            val fileNum = fileCounters((workerIp, workerPort))
            fileCounters((workerIp, workerPort)) = fileNum + 1
            
            val part1FileName = s"$from-$to-$fileNum"
            val part1FilePath = Paths.get(outputDir, part1FileName)
            writeRecordsToFile(part1FilePath.toString, part1Records)
            assignedFiles((workerIp, workerPort)) = assignedFiles((workerIp, workerPort)) :+ part1FilePath.toString
            
            println(s"[FileAssignment]   ✓ Created part1: $part1FileName (${part1Records.length} records)")
            
            // Part 2: push back to front of deque for next worker
            val tempFileName = s"temp_${System.nanoTime()}.bin"
            val part2FilePath = Paths.get(outputDir, tempFileName)
            writeRecordsToFile(part2FilePath.toString, part2Records)
            
            val part2StartKey = part2Records.head._1
            val part2EndKey = part2Records.last._1
            fileDeque.prepend((part2FilePath.toString, part2StartKey, part2EndKey))
            
            println(s"[FileAssignment]   ✓ Created part2: $tempFileName (${part2Records.length} records) - pushed to front")
            
            // Delete original file
            Files.deleteIfExists(Paths.get(filePath))
            
            // Move to next worker
            keepProcessing = false
            
          } else {
            // File is completely beyond this worker's range
            fileDeque.prepend((filePath, fileStartKey, fileEndKey))
            println(s"[FileAssignment]   File is beyond worker's range - pushed back ${new java.math.BigInteger(1, fileStartKey.toByteArray()).toString(16)} to ${new java.math.BigInteger(1, fileEndKey.toByteArray()).toString(16)} / ${new java.math.BigInteger(1, rangeStart.toByteArray()).toString(16)} to ${new java.math.BigInteger(1, rangeEnd.toByteArray()).toString(16)}")
            keepProcessing = false
          }
        }
      }
    }
    
    println(s"[FileAssignment] Assignment complete:")
    assignedFiles.foreach { case ((ip, port), files) =>
      println(s"  $ip:$port -> ${files.size} files")
    }
    
    assignedFiles.toMap
  }
  
  /**
   * Find the first index where key >= splitKey
   */
  private def findSplitIndex(
    records: Array[(ByteString, ByteString)],
    splitKey: ByteString
  ): Int = {
    val comparator = ByteString.unsignedLexicographicalComparator
    var left = 0
    var right = records.length
    
    while (left < right) {
      val mid = left + (right - left) / 2
      if (comparator.compare(records(mid)._1, splitKey) < 0) {
        left = mid + 1
      } else {
        right = mid
      }
    }
    
    left
  }
  
  /**
   * Load all records from a file into memory
   */
  private def loadFileRecords(filePath: String): Array[(ByteString, ByteString)] = {
    val channel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ)
    try {
      val fileSize = Files.size(Paths.get(filePath))
      val numRecords = (fileSize / RECORD_SIZE).toInt
      
      val records = Array.ofDim[(ByteString, ByteString)](numRecords)
      val keyBuffer = ByteBuffer.allocate(KEY_SIZE)
      val valueBuffer = ByteBuffer.allocate(VALUE_SIZE)
      val keyArray = new Array[Byte](KEY_SIZE)
      val valueArray = new Array[Byte](VALUE_SIZE)
      
      var i = 0
      while (i < numRecords) {
        keyBuffer.clear()
        valueBuffer.clear()
        
        channel.read(keyBuffer)
        channel.read(valueBuffer)
        
        keyBuffer.flip()
        valueBuffer.flip()
        
        keyBuffer.get(keyArray)
        valueBuffer.get(valueArray)
        
        records(i) = (ByteString.copyFrom(keyArray), ByteString.copyFrom(valueArray))
        i += 1
      }
      
      records
    } finally {
      channel.close()
    }
  }
  
  /**
   * Write records to a file
   */
  private def writeRecordsToFile(filePath: String, records: Array[(ByteString, ByteString)]): Unit = {
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

  /**
   * Get first and last key from a file
   */
  private def getFirstAndLastKeyFromFile(filePath: String): (ByteString, ByteString) = {
    val path = Paths.get(filePath)
    val channel = FileChannel.open(path, StandardOpenOption.READ)
    try {
      val buffer = ByteBuffer.allocate(KEY_SIZE)
      
      // Read first record
      channel.read(buffer)
      buffer.flip()
      val firstKeyBytes = new Array[Byte](KEY_SIZE)
      buffer.get(firstKeyBytes)
      val firstKey = ByteString.copyFrom(firstKeyBytes)
      
      // Read last record
      val fileSize = Files.size(path)
      channel.position(fileSize - RECORD_SIZE)
      buffer.clear()
      channel.read(buffer)
      buffer.flip()
      val lastKeyBytes = new Array[Byte](KEY_SIZE)
      buffer.get(lastKeyBytes)
      val lastKey = ByteString.copyFrom(lastKeyBytes)
      
      (firstKey, lastKey)
    } finally {
      channel.close()
    }
  }
}
