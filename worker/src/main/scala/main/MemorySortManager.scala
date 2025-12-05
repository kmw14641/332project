package main

import java.util.concurrent.{Executors, ConcurrentLinkedQueue}

import scala.concurrent.{Future, ExecutionContext}
import scala.jdk.CollectionConverters._
import scala.annotation.tailrec
import scala.async.Async.async
import scala.collection.mutable

import common.data.Data.{getRecordOrdering, RECORD_SIZE}
import global.StateRestoreManager
import utils.{ThreadpoolUtils, FileManager}
import utils.FileManager
import utils.FileManager.OutputSubDir
import state.MemorySortState

class MemorySortManager(outputSubDirName: String) {
  val threadPool = Executors.newFixedThreadPool(ThreadpoolUtils.getThreadCount)
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)
  implicit val outputSubDir: OutputSubDir = OutputSubDir(outputSubDirName)

  def start = {
    FileManager.createDirectoryIfNotExists(FileManager.getFilePathFromOutputDir(""))
    inMemorySort(ThreadpoolUtils.getChunkSize)
  }

  /**
   * In-memory sort of all input files
   * Split files into chunks if they exceed chunk size
   */
  private def inMemorySort(chunkSize: Long): Future[List[String]] = {
    // Collect all input files
    val allFiles = FileManager.getInputFilePaths
    
    println(s"[MergeSort] Found ${allFiles.size} input files to sort")
    
    val threadCount = ThreadpoolUtils.getThreadCount
    println(s"[MergeSort] Using $threadCount threads for sorting (max concurrent files: ${ThreadpoolUtils.getMaxConcurrentFiles})")
    
    val sortedFiles = new ConcurrentLinkedQueue[String]()
    
    val futures = allFiles.map { filePath =>
      async {
        if (MemorySortState.isFileProcessed(filePath)) {
          println(s"[StateRestore] Skip sorting for $filePath")
          val outputs = MemorySortState.getProcessedFiles(filePath)
          outputs.foreach(sortedFiles.add)
        } else {
          val threadId = Thread.currentThread().getName
          val fileSize = FileManager.getFilesize(filePath)
          val numRecords = fileSize / RECORD_SIZE
          
          println(s"[MergeSort-InMemory][$threadId] Processing file: $filePath (${numRecords} records)")
          
          val generatedFiles = mutable.ListBuffer.empty[String]

          // Process chunks recursively
          @tailrec
          def processChunks(offset: Long, chunkCount: Int): Unit = {
            if (offset < numRecords) {
              val recordsToRead = Math.min(chunkSize, numRecords - offset).toInt
              val currentChunk = chunkCount + 1
              
              println(s"[MergeSort-InMemory][$threadId] Reading chunk $currentChunk: $recordsToRead records from offset $offset")
              val records = FileManager.readRecords(filePath, offset, recordsToRead)
              
              println(s"[MergeSort-InMemory][$threadId] Sorting chunk $currentChunk...")
              implicit val ordering = getRecordOrdering
              val sortedRecords = records.sorted
              
              val outputFilename = FileManager.getRandomFilename
              val outputPath = FileManager.getFilePathFromOutputDir(outputFilename)
              FileManager.writeRecords(outputPath, sortedRecords)
              println(s"[MergeSort-InMemory][$threadId] Writing sorted chunk to: $outputFilename")
              generatedFiles += outputFilename
              
              processChunks(offset + recordsToRead, currentChunk)
            } else {
              println(s"[MergeSort-InMemory][$threadId] ✓ Completed file: $filePath ($chunkCount chunks)")
            }
          }
          
          processChunks(0L, 0)

          val outputList = generatedFiles.toList
          MemorySortState.markFileProcessed(filePath, outputList)
          StateRestoreManager.storeState()
          
          outputList.foreach(sortedFiles.add)
        }
      }
    }

    Future.sequence(futures)
    .map {
      case _ => 
        threadPool.shutdown()
        sortedFiles.asScala.toList
    }
  }
}
