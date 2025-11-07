import io.grpc.ServerBuilder
import scala.concurrent.ExecutionContext
import utils.{WorkerOptionUtils, PathUtils, SystemUtils}
import master.MasterClient
import worker.Worker

object Main extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global

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

  Worker.setMasterAddr(masterIp, masterPort)
  Worker.setInputDirs(inputDirs)
  Worker.setOutputDir(outputDir)

  val workerIp = SystemUtils.getLocalIp.getOrElse {
    println("Failed to get local IP address")
    sys.exit(1)
  }
  
  val ramMb = SystemUtils.getRamMb

  val server = ServerBuilder
    .forPort(0)
    .build()

  server.start()

  val port = server.getPort

  val client = new MasterClient(masterIp, masterPort)
  client.registerWorker(workerIp, port, ramMb)
}