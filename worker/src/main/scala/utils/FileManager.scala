package utils

import java.nio.ByteBuffer
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.channels.FileChannel
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}
import scala.annotation.tailrec

import com.google.protobuf.ByteString

import common.data.Data.{Record, KEY_SIZE, VALUE_SIZE, RECORD_SIZE}
import common.utils.SystemUtils

case class Metadata (
  filename: String,
  physicalFilePath: String,
)

object FileManager {
  val memSortDirName = "sorted"
  val fileMergeDirName = "merged"
  val labelingDirName = "labeled"
  val shuffleDirName = "shuffled"
  val finalDirName = "final"
  private val subDirNames = Set(memSortDirName, fileMergeDirName, labelingDirName, shuffleDirName, finalDirName)

  private var inputDirs: Seq[String] = Seq.empty
  private var outputDir: Option[String] = None

  def setInputDirs(dirs: Seq[String]) = this.synchronized {
    inputDirs = dirs
  }

  def setOutputDir(dir: String) = this.synchronized {
    outputDir = Some(dir)
  }

  def getInputFilePathes: Seq[String] = {
    if (inputDirs.isEmpty) throw new RuntimeException("Input directories are not set")
    else inputDirs.flatMap { dirPath =>
      Using(Files.list(Paths.get(dirPath))) { stream =>
        stream.iterator.asScala.map(_.toString).filter(path => Files.isRegularFile(Paths.get(path))).toSeq
      }.getOrElse(Seq.empty)
    }
  }

  def concatOutputPath(subDirName: String, filename: String): String = {
    require { subDirNames.contains(subDirName) }

    outputDir.map { outDir =>
      val subDirPath = Paths.get(outDir, subDirName)
      if (!Files.exists(subDirPath)) {
        Files.createDirectories(subDirPath)
      }
      subDirPath.resolve(filename).toString
    }.getOrElse {
      throw new RuntimeException("Output directory is not set")
    }
  }

  private val registry = new ConcurrentHashMap[String, Metadata]()

  @tailrec
  def getSafeFilename: String = {
    val filename = UUID.randomUUID().toString
    if (registry.containsKey(filename)) getSafeFilename
    else filename
  }

  def createAndRegister(subDirName: String, filenameOption: Option[String] = None): String = {
    require { subDirNames.contains(subDirName) }

    val filename = filenameOption.getOrElse { getSafeFilename }
    val physicalFilePath = concatOutputPath(subDirName, filename)

    registry.put(filename, Metadata(filename, physicalFilePath))
    Files.write(Paths.get(physicalFilePath), Array.emptyByteArray);

    filename
  }

  def getPhysicalFilePath(filename: String): String = {
    require { registry.containsKey(filename) }

    registry.get(filename).physicalFilePath
  }

  def getAllPhysicalFilePathes(filenames: Seq[String]): Seq[String] = {
    filenames.map { filename =>
      getPhysicalFilePath(filename)
    }
  }

  def getFilesize(filePath: String): Long = Files.size(Paths.get(filePath))

  /**
   * Read records from file starting at offset
   */
  def readRecords(filePath: String, offset: Long, count: Int): Array[Record] = {
    val file = Paths.get(filePath)

    Using(FileChannel.open(file, StandardOpenOption.READ)) { channel =>
      val records = Array.ofDim[Record](count)
      val keyBuffer = ByteBuffer.allocate(KEY_SIZE)
      val valueBuffer = ByteBuffer.allocate(VALUE_SIZE)
      
      var position = offset * RECORD_SIZE
      var i = 0
      while (i < count) {
        keyBuffer.clear()
        valueBuffer.clear()
        
        val keyBytesRead = channel.read(keyBuffer, position)
        val valueBytesRead = channel.read(valueBuffer, position + KEY_SIZE)
        
        if (keyBytesRead != KEY_SIZE || valueBytesRead != VALUE_SIZE) {
          throw new RuntimeException(s"Incomplete read at position $position in $filePath (key: $keyBytesRead/$KEY_SIZE, value: $valueBytesRead/$VALUE_SIZE)")
        }
        
        keyBuffer.flip()
        valueBuffer.flip()
        
        records(i) = (ByteString.copyFrom(keyBuffer), ByteString.copyFrom(valueBuffer))
        position += RECORD_SIZE
        i += 1
      }
      
      records
    }.get
  }

  /**
   * Write records to file
   */
  def writeRecords(subDirName: String, records: Array[Record], filenameOption: Option[String] = None): String = {
    require { subDirNames.contains(subDirName) }

    val filename = filenameOption.getOrElse(createAndRegister(subDirName))
    val filePath = concatOutputPath(subDirName, filename)

    Using(FileChannel.open(
      Paths.get(filePath),
      StandardOpenOption.WRITE,
      StandardOpenOption.TRUNCATE_EXISTING
    )) { channel =>
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
    }.get
    
    filename
  }

  def move(oldFilename: String, newFilename: String, subDirName: String): Unit = {
    require { subDirNames.contains(subDirName) }
    require { registry.containsKey(oldFilename) }
    require { oldFilename == newFilename || !registry.containsKey(newFilename) }

    val oldFilePath = getPhysicalFilePath(oldFilename)
    val newFilePath = concatOutputPath(subDirName, newFilename)

    Files.move(Paths.get(oldFilePath), Paths.get(newFilePath))

    registry.remove(oldFilename)
    registry.put(newFilename, Metadata(newFilename, newFilePath))
  }

  def delete(filename: String): Unit = {
    require { registry.containsKey(filename) }

    val filePath = getPhysicalFilePath(filename)
    Files.deleteIfExists(Paths.get(filePath))
    registry.remove(filename)
    println(s"[FileManager] Deleted file: $filePath")
  }

  def deleteAll(filenames: Seq[String]): Unit = {
    filenames.foreach { filename =>
      delete(filename)
    }
  }
}
