package main

import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ExecutorService}
import java.nio.file.{Files, Paths}
import java.nio.file.StandardOpenOption
import java.nio.channels.FileChannel
import java.nio.ByteBuffer

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.async.Async.{async, await}
import scala.util.Random
import scala.util.Using
import scala.async.Async.{async, await}
import scala.jdk.CollectionConverters._

import com.google.protobuf.ByteString

import common.data.Data.{Key, RECORD_SIZE, KEY_SIZE}
import common.utils.SystemUtils
import master.MasterService.{SamplingServiceGrpc, SampleData}
import master.MasterService.SampleData
import global.ConnectionManager
import global.FileManager
import global.StateRestoreManager
import global.WorkerState
import state.SampleState

class SampleManager(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  
  private val stub = SamplingServiceGrpc.stub(ConnectionManager.getMasterChannel())
  val SAMPLE_SIZE = Math.min(100000, SystemUtils.getRamMb * 1024 * 1024 / RECORD_SIZE)  // Number of samples to collect per worker, total 1MB
  val THREAD_NUM = Math.min(8, SystemUtils.getProcessorNum)

  def start(): Future[Unit] = async {
    if (SampleState.isSendSampleCompleted) {
        logger.info("Skip send sample")
    }
    else {
      val workerIp = SystemUtils.getLocalIp

      val samples = await { sampleFromInputs }
    
      val request = SampleData(
        workerIp = workerIp,
        keys = samples
      )

      await { stub.sampling(request) }

      logger.info("Samples sent successfully. Waiting for range assignment...")
    }
  }

  /**
   * Performs uniform sampling from input directories
   * Strategy: Sample records from multiple files without loading entire files into memory
   * 
   * @param inputDirs Sequence of input directory paths
   * @return Array of sampled 10-byte keys
   */
  def sampleFromInputs(implicit ec: ExecutionContext): Future[Array[Key]] = async {
    val allFiles = FileManager.getInputFilePaths.map {
      filePath => (filePath, FileManager.getFilesize(filePath) / RECORD_SIZE)
    }.filter(_._2 > 0)

    val random = new Random()
    val randomFileIdxAndRecordIdx = List.fill(SAMPLE_SIZE.toInt) {
      val fileIdx = random.nextInt(allFiles.size)
      val (filePath, numRecords) = allFiles(fileIdx)
      val recordIdx = random.nextInt(numRecords.toInt)
      (filePath, recordIdx)
    }.groupBy(_._1).mapValues(_.map(_._2).sorted).toMap

    val results = new ConcurrentLinkedQueue[Key]()
    implicit val gracefulExecutorReleasable: Using.Releasable[ExecutorService] = resource => {
      resource.shutdown()
    }

    val futures = Using(Executors.newFixedThreadPool(THREAD_NUM)) { threadPool =>
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)
      
      randomFileIdxAndRecordIdx.map { case (filePath, recordIdxs) =>
        async {
          val file = Paths.get(filePath)
          val channel = FileChannel.open(
            file, 
            StandardOpenOption.READ
          )
          
          val buffer = ByteBuffer.allocate(KEY_SIZE)
          recordIdxs.foreach { recordIdx =>
            val position = recordIdx.toLong * RECORD_SIZE
            try {
              channel.read(buffer, position)
              results.add(ByteString.copyFrom(buffer.array()))
            } catch {
              case e: Exception => 
                  logger.error(s"Error reading record at index $recordIdx from file $filePath: ${e.getMessage}")
            } finally {
              buffer.clear()
            }
          }
        }
      }

    }.get

    await { Future.sequence(futures) }
    results.asScala.toArray
  }
}