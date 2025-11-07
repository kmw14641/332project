import utils.{WorkerOptionUtils, PathUtils}

object Main extends App {
  val (masterAddr, inputDirs, outputDir) = WorkerOptionUtils.parse(args).getOrElse {
    sys.exit(1)
  }
  
  val (masterIp, masterPort) = {
    val parts = masterAddr.split(":")
    (parts(0), parts(1).toInt)
  }

  inputDirs.foreach {
    dir =>
      if (!PathUtils.exists(dir) || !PathUtils.isDirectory(dir)) {
        println(s"Input directory does not exist or is not a directory: $dir(${PathUtils.toAbsolutePath(dir)})")
        sys.exit(1)
      }
  }

  PathUtils.createDirectoryIfNotExists(outputDir)
}