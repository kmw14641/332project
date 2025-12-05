package main

import java.util.concurrent.{Executors, ConcurrentLinkedQueue}
import java.util.NoSuchElementException

import scala.concurrent.{Future, ExecutionContext}
import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer
import scala.async.Async.{async, await}
import scala.annotation.tailrec

import common.data.Data.{Record, getRecordOrdering, RECORD_SIZE}
import utils.{ThreadpoolUtils, FileManager}
import utils.FileManager.{InputSubDir, OutputSubDir}
import java.nio.file.Files
import java.nio.file.Paths
import state.FileMergeState
import global.StateRestoreManager

class FileMergeManager(inputSubDirName: String, outputSubDirName: String) {
  val threadPool = Executors.newFixedThreadPool(ThreadpoolUtils.getThreadCount)
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)
  implicit val inputSubDir: InputSubDir = InputSubDir(inputSubDirName)
  implicit val outputSubDir: OutputSubDir = OutputSubDir(outputSubDirName)

  def start(files: List[String], state: FileMergeState) = {
    // Local Merge와 Final Merge 두 개에서 이 Manager가 사용되기 떄문에 state를 인자로 받아야 한다.
    implicit val mergeState = state
    FileManager.createDirectoryIfNotExists(FileManager.getFilePathFromOutputDir(""))
    
    // 1. 만약 move가 안되었다
    // -> 그냥 평범하게 받은 files로 move 진행하면 된다
    // 2. 만약 move가 되었다.
    // -> 애초에 그럼 할 일이 없다. move가 되었다는 건 merge가 적어도 진행되기 직전이었다는 뜻이다.
    // merge의 진행은 전적으로 argument가 아닌 State의 fileLists를 의존하므로
    // move를 했다면 fileLists도 세팅이 되어있어야 함을 의미한다.
    // 따라서 그런 dependency를 고려하면 이 같은 로직이 옳다.
    if (!FileMergeState.isMoveCompleted) {
      val newFilenames = files.map { filename => {
        val newFilename = FileManager.getRandomFilename
        val oldFilePath = FileManager.getFilePathFromInputDir(filename)
        val newFilePath = FileManager.getFilePathFromOutputDir(newFilename)
        FileManager.move(oldFilePath, newFilePath)
        newFilename
      }}

      //sys.exit(1)

      FileMergeState.setCurrentFileLists(newFilenames.map(f => List(f)))
      FileMergeState.setRound(1)
      FileMergeState.setMoveCompleted
      StateRestoreManager.storeState()
    }
    
    twoWayMergeSort(state)
  }

  /**
   * 2-way merge sort
   * 
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
  private def twoWayMergeSort(implicit state: FileMergeState): Future[List[String]] = async {
    require { 
      FileMergeState.isMoveCompleted && 
      FileMergeState.getCurrentFileLists.isDefined &&
      FileMergeState.getRound >= 1
    }

    var currentLists = FileMergeState.getCurrentFileLists.get
    var round = FileMergeState.getRound

    while (currentLists.size > 1) {
      println(s"[MergeSort] Starting merge round $round with ${currentLists.size} file lists")
      
      // Pair up file lists
      val firstHalf = currentLists.take(currentLists.size / 2)
      val secondHalf = currentLists.drop(currentLists.size / 2)
      val listPairs = firstHalf.zip(secondHalf)
      
      // Handle remaining list if odd number
      val remainingList = if (currentLists.size % 2 == 1) {
        Some(currentLists.last)
      } else {
        None
      }
      
      // Filter out completed pairs
      val pendingPairs = listPairs.zipWithIndex.filterNot { case (_, index) => 
        FileMergeState.isPairCompleted(index)
      }
      
      val futures = pendingPairs.map { case ((list1, list2), index) =>
        async {
          val threadId = Thread.currentThread().getName
          println(s"[MergeSort-Round$round][$threadId] Merging list pair $index: ${list1.size} files + ${list2.size} files")
          val filePathList1 = FileManager.getFilePathFromOutputDirAll(list1)
          val filePathList2 = FileManager.getFilePathFromOutputDirAll(list2)
          val outputFiles = mergeFileLists(filePathList1, filePathList2, threadId, round)
          
          FileMergeState.markPairCompleted(index, outputFiles)
          StateRestoreManager.storeState()
    
          // Delete input files that were merged
          // 여기서 지우는 이유는 race condition 방지
          // 만약 merge가 완료된 후 지워진 상태에서 종료된다면 input을 삭제된 상태로 state 저장이 안 되어 복원에서 문제가 생길 수 있음
          // 여기서 지우면 적어도 state가 저장된 후 지우므로 어떤 경우에도 대응 가능함
          FileManager.deleteAll(filePathList1 ++ filePathList2)

          println(s"[MergeSort-Round$round][$threadId] Completed merge pair $index: ${outputFiles.size} output files")
          (index, outputFiles)
        }
      }

      await { Future.sequence(futures) }
      
      // Reconstruct results in order
      val results = (0 until listPairs.size).map { i =>
        FileMergeState.getCompletedPairResult(i)
      }.toList
      
      val nextFileLists = new ConcurrentLinkedQueue[List[String]]()
      results.foreach(nextFileLists.add)
      
      remainingList.foreach { list =>
        nextFileLists.add(list.map { file =>
          val oldFilePath = FileManager.getFilePathFromOutputDir(file)
          val newFilename = FileManager.getRandomFilename
          val newFilePath = FileManager.getFilePathFromOutputDir(newFilename)
          Files.move(Paths.get(oldFilePath), Paths.get(newFilePath))
          newFilename
        })
      }
      
      val nextLists = nextFileLists.asScala.toList
      
      // Update state for next round
      FileMergeState.setCurrentFileLists(nextLists)
      FileMergeState.clearCompletedPairs()
      FileMergeState.setRound(round + 1)
      StateRestoreManager.storeState()
      
      currentLists = nextLists
      round += 1
    }
    
    threadPool.shutdown()
    currentLists.flatten
  }

  private class MultiFileIterator(files: List[String]) extends Iterator[Record] {
    private var remainingFiles = files
    private var currentIter: Iterator[Record] = Iterator.empty

    private def loadIterator(filePath: String): BufferedIterator[Record] = {
      val numRecords = (FileManager.getFilesize(filePath) / RECORD_SIZE).toInt
      println(s"[MergeSort-Round] Loading file: $filePath ($numRecords records)")
      FileManager.readRecords(filePath, 0, numRecords).iterator.buffered
    }

    @tailrec
    final override def hasNext: Boolean = {
      if (currentIter.hasNext) true
      else if (remainingFiles.isEmpty) false
      else {
        val nextFile = remainingFiles.head
        remainingFiles = remainingFiles.tail
        currentIter = loadIterator(nextFile)
        hasNext
      }
    }

    override def next(): Record = {
      if (hasNext) currentIter.next()
      else throw new NoSuchElementException("next on empty iterator")
    }
  }

  // TODO: threadId/round are only for logging, later deletes them
  /**
   * Merge two lists of sorted files in memory
   * Files are loaded entirely into memory and merged
   * 
   * ensures: output files are sorted in order
   */
  private def mergeFileLists(
    filePathList1: List[String],
    filePathList2: List[String],
    threadId: String,
    round: Int
  ): List[String] = {
    val comparator = getRecordOrdering
    
    // 1. Calculate average file size (in records)
    val allFiles = filePathList1 ++ filePathList2
    val totalRecords = allFiles.map(FileManager.getFilesize).sum / RECORD_SIZE
    val avgRecordsPerFile = Math.ceil(totalRecords.toDouble / allFiles.size).toInt
    val targetChunkSize = Math.max(avgRecordsPerFile, 1)
    
    println(s"[MergeSort-Round$round][$threadId] Merging ${filePathList1.size} + ${filePathList2.size} files, avg chunk size: $targetChunkSize records")

    val iter1 = new MultiFileIterator(filePathList1).buffered
    val iter2 = new MultiFileIterator(filePathList2).buffered

    @tailrec
    def mergeLoop(
      buffer: ArrayBuffer[Record],
      outputFiles: List[String]
    ): List[String] = {
      assert { buffer.size <= targetChunkSize }
      
      if (buffer.size == targetChunkSize) {
        val outputFilename = FileManager.getRandomFilename
        val outputPath = FileManager.getFilePathFromOutputDir(outputFilename)
        FileManager.writeRecords(outputPath, buffer.toArray)
        println(s"[MergeSort-Round$round][$threadId] Writing output file: $outputPath (${buffer.size} records)")
        buffer.clear()
        mergeLoop(buffer, outputFilename :: outputFiles)
      } else if (iter1.hasNext && iter2.hasNext) {
        if (comparator.compare(iter1.head, iter2.head) <= 0) buffer += iter1.next()
        else buffer += iter2.next()

        mergeLoop(buffer, outputFiles)
      } else if (iter1.hasNext) {
        while (iter1.hasNext && buffer.size < targetChunkSize) buffer += iter1.next()

        mergeLoop(buffer, outputFiles)
      } else if (iter2.hasNext) {
        while (iter2.hasNext && buffer.size < targetChunkSize) buffer += iter2.next()
        
        mergeLoop(buffer, outputFiles)
      } else {
        if (buffer.nonEmpty) {
          val outputFilename = FileManager.getRandomFilename
          val outputPath = FileManager.getFilePathFromOutputDir(outputFilename)
          FileManager.writeRecords(outputPath, buffer.toArray)
          println(s"[MergeSort-Round$round][$threadId] Writing output file: $outputPath (${buffer.size} records)")
          (outputFilename :: outputFiles).reverse
        } else {
          outputFiles.reverse
        }
      }
    }
    
    val result = mergeLoop(ArrayBuffer.empty, Nil)
    
    println(s"[MergeSort-Round$round][$threadId] Merge complete: ${result.size} output files")
    result
  }
}
