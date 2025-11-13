package utils

import java.nio.file.{Files, Paths}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.util.concurrent.{ConcurrentLinkedQueue, Executors}
import scala.jdk.CollectionConverters._
import scala.util.Random
import scala.concurrent.{Future, ExecutionContext, Await, duration}
import com.google.protobuf.ByteString
import utils.PathUtils
import common.utils.SystemUtils

object SamplingUtils {
  val RECORD_SIZE = 100  // 10 bytes key + 90 bytes value
  val KEY_SIZE = 10
  val SAMPLE_SIZE = Math.min(100000, SystemUtils.getRamMb * 1024 * 1024 / RECORD_SIZE)  // Number of samples to collect per worker, total 1MB
  val THREAD_NUM = Math.min(8, Runtime.getRuntime.availableProcessors())

  /**
   * Performs uniform sampling from input directories
   * Strategy: Sample records from multiple files without loading entire files into memory
   * 
   * @param inputDirs Sequence of input directory paths
   * @return Array of sampled 10-byte keys
   */
  def sampleFromInputs(inputDirs: Seq[String]): Option[Array[ByteString]] = {
    require {
      inputDirs.forall { dirPath => PathUtils.exists(dirPath) && PathUtils.isDirectory(dirPath) }
    }

    val allFiles = inputDirs.flatMap { dirPath =>
      val dir = Paths.get(dirPath)
      Files.list(dir).iterator().asScala
        .filter(p => Files.isRegularFile(p) && Files.size(p) >= RECORD_SIZE)
        .map(p => (p.toString, Files.size(p) / RECORD_SIZE))
        .toList
    }
    if (allFiles.isEmpty) {
      println("Warning: No files large enough for sampling")
      return None
    }

    val random = new Random()
    val randomFileIdxAndRecordIdx = List.fill(SAMPLE_SIZE.toInt) {
      val fileIdx = random.nextInt(allFiles.size)
      val (filePath, numRecords) = allFiles(fileIdx)
      val recordIdx = random.nextInt(numRecords.toInt)
      (filePath, recordIdx)
    }.groupBy(_._1).mapValues(_.map(_._2)).toMap

    val results = new ConcurrentLinkedQueue[ByteString]()
    val threadPool = Executors.newFixedThreadPool(THREAD_NUM)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

    val futures = randomFileIdxAndRecordIdx.map { case (filePath, recordIdxs) =>
      Future {
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
              println(s"Error reading record at index $recordIdx from file $filePath: ${e.getMessage}")
          } finally {
            buffer.clear()
          }
        }
      }
    }

    try {
      Await.result(Future.sequence(futures), duration.Duration.Inf)
    }
    finally {
      threadPool.shutdown()
    }

    Some(results.asScala.toArray)
  }
}
