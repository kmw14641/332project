package utils

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths, StandardOpenOption}
import com.google.protobuf.ByteString
import common.utils.SystemUtils
import common.data.Data.{Record, KEY_SIZE, VALUE_SIZE, RECORD_SIZE}

object ThreadpoolUtils {
  val MAX_CHUNK_SIZE_MB = 32  // Upper bound: 32 MiB
  val MEMORY_OVERHEAD_FACTOR = 2.2 // Record overhead, almost 2.2x memory usage

  /**
   * Calculate chunk size: at least 3 files should fit in RAM, with upper bound of 32MiB
   */
  def getChunkSize: Long = {
    val ramBytes = SystemUtils.getRamMb * 1024 * 1024
    val maxChunkBytes = MAX_CHUNK_SIZE_MB * 1024 * 1024
    // Ensure at least 3 chunks can fit in RAM (use 80% of RAM)
    val chunkBytesForRam = (ramBytes * 0.8 / (3 * MEMORY_OVERHEAD_FACTOR)).toLong
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
    // we need 3 files per merge operation (2 input + 1 output)
    val maxFiles = (ramBytes * 0.8 / (chunkBytes * MEMORY_OVERHEAD_FACTOR)).toInt
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
}
