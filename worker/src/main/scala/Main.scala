import io.grpc.ServerBuilder
import scala.concurrent.ExecutionContext
import utils.{WorkerOptionUtils, PathUtils, SamplingUtils}
import server.{WorkerServiceImpl}
import global.WorkerState
import worker.WorkerService.WorkerServiceGrpc
import scala.concurrent.Future
import common.utils.SystemUtils
import global.ConnectionManager
import main.RegisterManager
import main.SampleManager

object Main extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global

  val (masterAddr, inputDirs, outputDir) = WorkerOptionUtils.parse(args).getOrElse {
    sys.exit(1)
  }
  
  val (masterIp, masterPort) = {
    val parts = masterAddr.split(":")
    (parts(0), parts(1).toInt)
  }

  val invalidInputDirs = inputDirs.filter{ dir => !PathUtils.exists(dir) || !PathUtils.isDirectory(dir) }

  if (invalidInputDirs.nonEmpty) {
    invalidInputDirs.foreach { dir =>
      Console.err.println(s"Input directory does not exist or is not a directory: $dir(${PathUtils.toAbsolutePath(dir)})")
    }
    sys.exit(1)
  }

  PathUtils.createDirectoryIfNotExists(outputDir)

  WorkerState.setMasterAddr(masterIp, masterPort)
  WorkerState.setInputDirs(inputDirs)
  WorkerState.setOutputDir(outputDir)

  val server = ServerBuilder
    .forPort(0)
    .addService(WorkerServiceGrpc.bindService(new WorkerServiceImpl(), ec))
    .build()

  server.start()

  ConnectionManager.initMasterChannel(masterIp, masterPort)

  new RegisterManager().start(server.getPort)

  new SampleManager().start()

  ConnectionManager.shutdownAllChannels()

  server.awaitTermination()
}