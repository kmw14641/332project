package main

import org.slf4j.LoggerFactory
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.channels.FileChannel
import java.nio.ByteBuffer

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.async.Async.async

import com.google.protobuf.ByteString

import common.utils.SystemUtils
import common.data.Data.{Key, Record, getRecordOrdering, getKeyOrdering, RECORD_SIZE, KEY_SIZE}
import global.FileManager
import global.FileManager.{InputSubDir, OutputSubDir}

class LabelingManager(inputSubDirName: String, outputSubDirName: String, assignedRange: Map[(String, Int), (Key, Key)])(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit val inputSubDirNameImplicit: InputSubDir = InputSubDir(inputSubDirName)
  implicit val outputSubDirNameImplicit: OutputSubDir = OutputSubDir(outputSubDirName)

  def start(files: List[String]) = {
    assignFilesToWorkers(files)
  }

  /**
   * Assign sorted files to workers based on assigned ranges
   * 
   * @param files List of sorted file paths in order
   * @param assignedRange Map of (workerIp, workerPort) -> (startKey, endKey)
   * @param outputDir Directory to write assigned files
   * @return Map of (workerIp, workerPort) -> List[filePath]
   */
  private def assignFilesToWorkers(files: List[String]): Future[Map[(String, Int), List[String]]] = async {
    logger.info(s"[FileAssignment] Starting file assignment with ${files.size} sorted files")
    
    // Sort workers by start key
    implicit val cp = getRecordOrdering
    val sortedWorkers = assignedRange.toList.sorted
    
    logger.info(s"[FileAssignment] Worker ranges (sorted):")
    sortedWorkers.foreach { case ((ip, port), (start, end)) =>
      logger.info(s"  $ip:$port -> [${new java.math.BigInteger(1, start.toByteArray()).toString(16)}, ${new java.math.BigInteger(1, end.toByteArray()).toString(16)})")
    }
    
    // Get file metadata (filename, startKey, endKey) in sorted order
    val fileMetadata = files.map { filename =>
      val filePath = FileManager.getFilePathFromInputDir(filename)
      val (firstKey, lastKey) = getFirstAndLastKeyFromFile(filePath)
      (filename, firstKey, lastKey)
    }
    
    val from = SystemUtils.getLocalIp.getOrElse(throw new IllegalStateException("Could not determine local IP address"))
    val comparator = getKeyOrdering
    
    @tailrec
    def processFiles(
      workerId: (String, Int),
      rangeStart: Key,
      rangeEnd: Key,
      files: List[(String, Key, Key)],
      assignments: Map[(String, Int), List[String]]
    ): (List[(String, Key, Key)], Map[(String, Int), List[String]]) = {
      files match {
        case Nil => (Nil, assignments)
        case (currentFile @ (filename, fileStartKey, fileEndKey)) :: restFiles =>
          val (workerIp, _) = workerId
          val filePath = FileManager.getFilePathFromInputDir(filename)
          logger.info(s"[FileAssignment]   Checking file: $filename")
          
          // Check if file's end key is within [rangeStart, rangeEnd)
          val fileEndInRange = comparator.compare(fileEndKey, rangeStart) >= 0 && 
                               comparator.compare(fileEndKey, rangeEnd) < 0
          
          if (fileEndInRange) {
            // Entire file belongs to this worker - just rename
            val fileNum = assignments.getOrElse(workerId, List.empty).size
            val newFilename = s"$from-$workerIp-$fileNum"
            val newFilePath = FileManager.getFilePathFromOutputDir(newFilename)
            
            FileManager.move(filePath, newFilePath)
            logger.info(s"[FileAssignment]   ✓ Renamed entire file to: $newFilename")
            
            val newAssignments = assignments.updated(workerId, newFilename :: assignments.getOrElse(workerId, List.empty))
            
            processFiles(workerId, rangeStart, rangeEnd, restFiles, newAssignments)
            
          } else {
            // File's end key is beyond this worker's range - need to split
            // Check if rangeEnd falls within this file
            val rangeEndInFile = comparator.compare(rangeEnd, fileStartKey) > 0 && 
                                 comparator.compare(rangeEnd, fileEndKey) <= 0
            
            if (rangeEndInFile) {
              // Split file at rangeEnd
              logger.info(s"[FileAssignment]   Splitting file at rangeEnd...")
              
              // Load file into memory and find split point
              val count = FileManager.getFilesize(filePath) / RECORD_SIZE
              val records = FileManager.readRecords(filePath, 0, count.toInt)
              val splitIndex = findSplitIndex(records, rangeEnd)
              
              // Split into two parts
              val part1Records = records.take(splitIndex)
              val part2Records = records.drop(splitIndex)
              
              // Part 1: belongs to current worker
              val fileNum = assignments.getOrElse(workerId, List.empty).size
              val newFilename = s"$from-$workerIp-$fileNum"
              val newFilePath = FileManager.getFilePathFromOutputDir(newFilename)
              FileManager.writeRecords(newFilePath, part1Records)
              logger.info(s"[FileAssignment]   ✓ Created part1: $newFilename (${part1Records.length} records)")
              
              val newAssignments = assignments.updated(workerId, newFilename :: assignments.getOrElse(workerId, List.empty))
              
              // Part 2: push back to front of deque for next worker
              val remainingFilename = FileManager.getRandomFilename
              val remainingFilePath = FileManager.getFilePathFromInputDir(remainingFilename)
              FileManager.writeRecords(remainingFilePath, part2Records)
              
              val remainingStartKey = part2Records.head._1
              val remainingEndKey = part2Records.last._1
              logger.info(s"[FileAssignment]   ✓ Created part2: $remainingFilename (${part2Records.length} records) - pushed to front")
              
              // Delete original file
              FileManager.delete(filePath)
              
              // Return remaining files with part2 prepended, and stop processing for this worker
              ((remainingFilename, remainingStartKey, remainingEndKey) :: restFiles, newAssignments)
              
            } else {
              // File is completely beyond this worker's range
              logger.info(s"[FileAssignment]   File is beyond worker's range - pushed back")
              (files, assignments)
            }
          }
      }
    }

    val finalAssignments = sortedWorkers.foldLeft((fileMetadata, Map.empty[(String, Int), List[String]])) {
      case ((files, assignments), (workerId, (rangeStart, rangeEnd))) =>
        val (remainingFiles, newAssignments) = processFiles(workerId, rangeStart, rangeEnd, files, assignments)
        logger.info(s"[FileAssignment] Processing worker ${workerId._1}")
        (remainingFiles, newAssignments)
    }
    val result = finalAssignments._2.map { case (k, v) => k -> v.reverse }
    
    logger.info(s"[FileAssignment] Assignment complete:")
    result.foreach { case ((ip, port), files) =>
      logger.info(s"  $ip:$port -> ${files.size} files")
    }
    
    result
  }
  
  /**
   * Find the first index where key >= splitKey
   */
  private def findSplitIndex(
    records: Array[Record],
    splitKey: Key
  ): Int = {
    val comparator = getKeyOrdering
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
  
  // Removed loadFileRecords and writeRecordsToFile as they are now in RecordIOUtils

  /**
   * Get first and last key from a file
   */
  private def getFirstAndLastKeyFromFile(filePath: String): (Key, Key) = {
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
