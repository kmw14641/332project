package utils

import java.nio.file.{Paths, Files}
import java.io.File

object PathUtils {
  def toAbsolutePath(inputPath: String): String = Paths.get(inputPath).toAbsolutePath().toString
  def exists(inputPath: String): Boolean = Files.exists(Paths.get(inputPath))
  def isDirectory(inputPath: String): Boolean = Files.isDirectory(Paths.get(inputPath))
  def createDirectoryIfNotExists(dirPath: String): Unit = {
    val path = Paths.get(dirPath)
    if (!Files.exists(path)) {
      Files.createDirectories(path)
    }
  }
  def getFilesList(dirPath: String): List[String] =
    Option(new File(dirPath).listFiles())
      .map(_.filter(_.isFile))
      .map(_.map(_.getName).toList)
      .getOrElse(List.empty[String])
}