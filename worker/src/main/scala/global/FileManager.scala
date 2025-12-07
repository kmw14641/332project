package global

import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.nio.file.{Files, Paths, StandardOpenOption, StandardCopyOption}
import java.nio.channels.FileChannel
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}
import scala.annotation.tailrec

import com.google.protobuf.ByteString

import common.data.Data.{Record, KEY_SIZE, VALUE_SIZE, RECORD_SIZE}
import common.utils.SystemUtils

object FileManager {
  private val logger = LoggerFactory.getLogger(getClass)
  private val BUFFER_SIZE = 8 * 1024 // 8KiB

  case class InputSubDir(val value: String)
  case class OutputSubDir(val value: String)

  val memSortDirName = "sorted"
  val fileMergeDirName = "merged"
  val labelingDirName = "labeled"
  val shuffleDirName = "shuffled"
  val finalDirName = "final"
  val stateRestoreDirName = "state"
  private val inputSubDirNames = Set(memSortDirName, fileMergeDirName, labelingDirName, shuffleDirName, finalDirName, stateRestoreDirName)
                                  .map(InputSubDir(_))
  private val outputSubDirNames = Set(memSortDirName, fileMergeDirName, labelingDirName, shuffleDirName, finalDirName, stateRestoreDirName)
                                  .map(OutputSubDir(_))

  private var inputDirs: Seq[String] = Seq.empty
  private var outputDir: Option[String] = None

  def setInputDirs(dirs: Seq[String]) = this.synchronized {
    inputDirs = dirs
  }

  def setOutputDir(dir: String) = this.synchronized {
    outputDir = Some(dir)
  }

  def getInputDirs = inputDirs

  def getOutputDir = outputDir

  def getInputFilePaths: Seq[String] = {
    if (inputDirs.isEmpty) throw new RuntimeException("Input directories are not set")
    else inputDirs.flatMap { dirPath =>
      Using(Files.list(Paths.get(dirPath))) { stream =>
        stream.iterator.asScala.map(_.toString).filter(path => Files.isRegularFile(Paths.get(path))).toSeq
      }.getOrElse(Seq.empty)
    }
  }

  def getFilePathFromInputDir(filename: String)(implicit inputSubDir: InputSubDir): String = {
    require { inputSubDirNames.contains(inputSubDir) }

    outputDir match {
      case Some(outputDirectory) => Paths.get(outputDirectory, inputSubDir.value, filename).toString
      case None => throw new RuntimeException("Output directory are not set")
    }
  }

  def getFilePathFromInputDirAll(filenames: Seq[String])(implicit inputSubDir: InputSubDir): List[String] = {
    require { inputSubDirNames.contains(inputSubDir) }

    filenames.map(getFilePathFromInputDir).toList
  }

  def getFilePathFromOutputDir(filename: String)(implicit outputSubDir: OutputSubDir): String = {
    require { outputSubDirNames.contains(outputSubDir) }

    outputDir match {
      case Some(outputDirectory) => Paths.get(outputDirectory, outputSubDir.value, filename).toString
      case None => throw new RuntimeException("Output directory are not set")
    }
  }

  def getFilePathFromOutputDirAll(filenames: Seq[String])(implicit outputSubDir: OutputSubDir): List[String] = {
    require { outputSubDirNames.contains(outputSubDir) }

    filenames.map(getFilePathFromOutputDir).toList
  }

  def getRandomFilename: String = UUID.randomUUID().toString
  
  def createDirectoryIfNotExists(dirPath: String): Unit = Files.createDirectories(Paths.get(dirPath))

  def getFilesize(filePath: String): Long = Files.size(Paths.get(filePath))

  /**
   * Read records from file starting at offset
   */
  def readRecords(filePath: String, offset: Long, count: Int): Array[Record] = {
    val file = Paths.get(filePath)

    Using(FileChannel.open(file, StandardOpenOption.READ)) { channel =>
      val records = Array.ofDim[Record](count)
      val buffer = ByteBuffer.allocateDirect(BUFFER_SIZE)
      
      channel.position(offset * RECORD_SIZE)
      buffer.flip()
      
      val keyBytes = new Array[Byte](KEY_SIZE)
      val valueBytes = new Array[Byte](VALUE_SIZE)
      
      var i = 0
      while (i < count) {
        if (buffer.remaining() < RECORD_SIZE) {
          buffer.compact()
          val bytesRead = channel.read(buffer)
          buffer.flip()
          if (buffer.remaining() < RECORD_SIZE) {
            throw new RuntimeException(s"Unexpected EOF or partial record in $filePath")
          }
        }
        
        buffer.get(keyBytes)
        buffer.get(valueBytes)
        
        records(i) = (ByteString.copyFrom(keyBytes), ByteString.copyFrom(valueBytes))
        i += 1
      }
      
      records
    }.get
  }

  /**
   * Write records to file
   */
  def writeRecords(filePath: String, records: Array[Record]): Unit = {
    Using(FileChannel.open(
      Paths.get(filePath),
      StandardOpenOption.CREATE,
      StandardOpenOption.WRITE,
      StandardOpenOption.TRUNCATE_EXISTING
    )) { channel =>
      val buffer = ByteBuffer.allocateDirect(BUFFER_SIZE)
      
      var i = 0
      while (i < records.length) {
        val (key, value) = records(i)
        
        if (buffer.remaining() < RECORD_SIZE) {
          buffer.flip()
          while (buffer.hasRemaining) {
            channel.write(buffer)
          }
          buffer.clear()
        }
        
        key.copyTo(buffer)
        value.copyTo(buffer)
        
        i += 1
      }
      
      if (buffer.position() > 0) {
        buffer.flip()
        while (buffer.hasRemaining) {
          channel.write(buffer)
        }
      }
    }.get
  }

  def link(oldFilePath: String, newFilePath: String): Unit = Files.createLink(Paths.get(newFilePath), Paths.get(oldFilePath))

  def copy(oldFilePath: String, newFilePath: String): Unit = Files.copy(
    Paths.get(oldFilePath),
    Paths.get(newFilePath),
    StandardCopyOption.REPLACE_EXISTING
  )

  def delete(filePath: String): Unit = Files.deleteIfExists(Paths.get(filePath))

  def deleteAll(filePaths: Seq[String]): Unit = {
    filePaths.foreach { filename => Try { delete(filename) } }
  }

  def createAllDirIfNotExists: Unit = {
    outputDir.foreach { outDir => 
      FileManager.createDirectoryIfNotExists(outDir)
      outputSubDirNames.map(_.value).foreach { subDirName =>
        val subDirPath = Paths.get(outDir, subDirName).toString()
        FileManager.createDirectoryIfNotExists(subDirPath)
      }
    }
  }

  def deleteAllIntermedia: Unit = {
    outputSubDirNames.map(_.value).foreach { subDirName =>
      outputDir.foreach { outDir =>
        val subDirPath = Paths.get(outDir, subDirName)
        if (Files.exists(subDirPath) && Files.isDirectory(subDirPath)) {
          try {
            Files.walk(subDirPath)
              .sorted(java.util.Comparator.reverseOrder())
              .forEach { path =>
                Files.deleteIfExists(path)
              }
            logger.info(s"Deleted sub-directory: $subDirPath")
          } catch {
            case e: Exception =>
              logger.warn(s"Failed to delete sub-directory $subDirPath: ${e.getMessage}")
          }
        }

      }
    }
  }

  def mergeAllFiles(outputPath: String, filenames: Seq[String], inputSubDirName: String): Unit = {
    implicit val inputSubDir: InputSubDir = InputSubDir(inputSubDirName)
    require { inputSubDirNames.contains(inputSubDir) }
    
    val filePaths = getFilePathFromInputDirAll(filenames)
    Using(
      FileChannel.open(
        Paths.get(outputPath),
        StandardOpenOption.CREATE, 
        StandardOpenOption.WRITE, 
        StandardOpenOption.TRUNCATE_EXISTING
      )
    ) { destChannel =>
      filePaths.foreach { filePath =>
        Using(FileChannel.open(Paths.get(filePath), StandardOpenOption.READ)) { srcChannel =>
          val size = srcChannel.size()
          var position: Long = 0
          while (position < size) {
            val transferred = srcChannel.transferTo(position, size - position, destChannel)
            position += transferred
            if (transferred == 0) {
              throw new RuntimeException(s"Failed to transfer data from $filePath to $outputPath")
            }
          }
        }.get
      }
    }.get

    logger.info(s"Creating Output Completed - Final output file: $outputPath")
  }
}
