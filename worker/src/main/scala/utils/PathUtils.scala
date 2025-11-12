package utils

import scala.util.Try
import scala.jdk.CollectionConverters._
import java.nio.file.{Paths, Files}

object PathUtils {
  def toAbsolutePath(inputPath: String): String = Paths.get(inputPath).toAbsolutePath().toString
  def exists(inputPath: String): Boolean = Files.exists(Paths.get(inputPath))
  def isDirectory(inputPath: String): Boolean = Files.isDirectory(Paths.get(inputPath))
  def createDirectoryIfNotExists(dirPath: String): Unit = Files.createDirectories(Paths.get(dirPath))
  def getFilesList(dirPath: String): List[String] = {
    Try {
      val stream = Files.list(Paths.get(dirPath))
      try {
        stream.iterator.asScala.map(_.toString).toList
      } finally {
        stream.close()
      }
    }.getOrElse(List.empty)
  }
}