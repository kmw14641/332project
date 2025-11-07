import utils.WorkerOptionUtils

object Main extends App {
  val (masterAddr, inputDirs, outputDir) = WorkerOptionUtils.parse(args).getOrElse {
    sys.exit(1)
  }
}