package main

import java.util.concurrent.{Executors, ConcurrentLinkedQueue}
import utils.{ThreadpoolUtils, FileManager}
import scala.concurrent.{Future, ExecutionContext}
import scala.jdk.CollectionConverters._
import scala.annotation.tailrec
import scala.async.Async.async
import common.data.Data.{Record, getRecordOrdering, RECORD_SIZE}
import utils.FileManager

class MemorySortManager(subDirName: String) {
  val threadPool = Executors.newFixedThreadPool(ThreadpoolUtils.getThreadCount)
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

  def start = {
    inMemorySort(ThreadpoolUtils.getChunkSize)
  }

  /**
   * In-memory sort of all input files
   * Split files into chunks if they exceed chunk size
   */
  private def inMemorySort(chunkSize: Long): Future[List[String]] = {
    // Collect all input files
    val allFiles = FileManager.getInputFilePathes
    
    println(s"[MergeSort] Found ${allFiles.size} input files to sort")
    
    val threadCount = ThreadpoolUtils.getThreadCount
    println(s"[MergeSort] Using $threadCount threads for sorting (max concurrent files: ${ThreadpoolUtils.getMaxConcurrentFiles})")
    
    val sortedFiles = new ConcurrentLinkedQueue[String]()
    
    val futures = allFiles.map { filePath =>
      async {
        val threadId = Thread.currentThread().getName
        val fileSize = FileManager.getFilesize(filePath)
        val numRecords = fileSize / RECORD_SIZE
        
        println(s"[MergeSort-InMemory][$threadId] Processing file: $filePath (${numRecords} records)")
        
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
            
            val outputFilename = FileManager.writeRecords(subDirName, sortedRecords)
            println(s"[MergeSort-InMemory][$threadId] Writing sorted chunk to: $outputFilename")
            sortedFiles.add(outputFilename)
            
            processChunks(offset + recordsToRead, currentChunk)
          } else {
            println(s"[MergeSort-InMemory][$threadId] ✓ Completed file: $filePath ($chunkCount chunks)")
          }
        }
        
        processChunks(0L, 0)
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
