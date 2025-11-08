package utils

import java.io.RandomAccessFile
import java.nio.file.{Files, Paths}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import scala.jdk.CollectionConverters._
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import com.google.protobuf.ByteString

object SamplingUtils {
  val RECORD_SIZE = 100  // 10 bytes key + 90 bytes value
  val KEY_SIZE = 10
  val SAMPLE_SIZE = 100000  // Number of samples to collect per worker, total 1MB

  /**
   * Performs uniform sampling from input directories
   * Strategy: Sample records from multiple files without loading entire files into memory
   * 
   * @param inputDirs Sequence of input directory paths
   * @return Array of sampled 10-byte keys
   */
  def sampleFromInputs(inputDirs: Seq[String]): Option[Array[ByteString]] = {
    val allFiles = inputDirs.flatMap { dirPath =>
      val dir = Paths.get(dirPath)
      if (Files.exists(dir) && Files.isDirectory(dir)) {
        Files.list(dir).iterator().asScala
          .filter(p => Files.isRegularFile(p))
          .map(_.toString)
          .toList
      } else {
        List.empty[String]
      }
    }
    if (allFiles.isEmpty) {
      println("Warning: No files large enough for sampling")
      return None
    }

    val buffer = new ArrayBuffer[ByteString]()
    val random = new Random()
    while (buffer.size < SAMPLE_SIZE) {
      val randomIdx = random.nextInt(allFiles.size)
      val filePath = allFiles(randomIdx)
      val file = Paths.get(filePath)
      val fileSize = Files.size(file)
      val bytesToRead = Math.min(SAMPLE_SIZE - buffer.size, fileSize / RECORD_SIZE) * RECORD_SIZE
      val channel = FileChannel.open(
        file, 
        StandardOpenOption.READ
      )
      val tmp_buffer = ByteBuffer.allocate(bytesToRead.toInt)

      try {
        channel.read(tmp_buffer, 0)
      } finally {
        channel.close()
      }

      tmp_buffer.array().grouped(RECORD_SIZE).foreach { record =>
        if (buffer.size < SAMPLE_SIZE) {
          val keyBytes = record.slice(0, KEY_SIZE)
          buffer.append(ByteString.copyFrom(keyBytes))
        }
      }
    }

    Some(buffer.toArray)
  }
}
