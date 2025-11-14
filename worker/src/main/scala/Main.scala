import io.grpc.ServerBuilder
import scala.concurrent.ExecutionContext
import utils.{WorkerOptionUtils, PathUtils, SamplingUtils}
import master.MasterClient
import worker.{Worker, WorkerServiceImpl}
import worker.WorkerService.WorkerServiceGrpc
import scala.concurrent.Future
import common.utils.SystemUtils

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

  Worker.setMasterAddr(masterIp, masterPort)
  Worker.setInputDirs(inputDirs)
  Worker.setOutputDir(outputDir)

  val server = ServerBuilder
    .forPort(0)
    .addService(WorkerServiceGrpc.bindService(new WorkerServiceImpl(), ec))
    .build()

  server.start()

  val workerIp = SystemUtils.getLocalIp.getOrElse {
    println("Failed to get local IP address")
    sys.exit(1)
  }
  val ramMb = SystemUtils.getRamMb
  val port = server.getPort

  val client = new MasterClient(masterIp, masterPort)
  client.registerWorker(workerIp, port, ramMb)

  // Start sampling in a separate thread
  val samplingPhase = Future {
    try {
      val samples = SamplingUtils.sampleFromInputs(inputDirs).getOrElse {
        println("Warning: Sampling failed")
        sys.exit(1)
      }
      
      val success = client.sampling(workerIp, samples)
      
      if (success) {
        println("Samples sent successfully. Waiting for range assignment...")
      } else {
        println("Failed to send samples to master")
      }
    } catch {
      case e: Exception =>
        println(s"Error during sampling: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  server.awaitTermination()
}