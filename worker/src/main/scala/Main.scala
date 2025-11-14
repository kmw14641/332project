import io.grpc.ServerBuilder
import scala.concurrent.ExecutionContext
import utils.{WorkerOptionUtils, PathUtils, SystemUtils, SamplingUtils}
import master.MasterClient
import worker.{Worker, WorkerServiceImpl}
import worker.WorkerService.WorkerServiceGrpc
import server.ShuffleServiceImpl
import shuffle.Shuffle.ShuffleGrpc
import scala.concurrent.Future
import client.ShuffleClient
import com.google.protobuf.ByteString

object Main extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global

  // val (masterAddr, inputDirs, outputDir) = WorkerOptionUtils.parse(args).getOrElse {
  //   sys.exit(1)
  // }
  
  // val (masterIp, masterPort) = {
  //   val parts = masterAddr.split(":")
  //   (parts(0), parts(1).toInt)
  // }

  // inputDirs.foreach {
  //   dir =>
  //     if (!PathUtils.exists(dir) || !PathUtils.isDirectory(dir)) {
  //       println(s"Input directory does not exist or is not a directory: $dir(${PathUtils.toAbsolutePath(dir)})")
  //       sys.exit(1)
  //     }
  // }

  // PathUtils.createDirectoryIfNotExists(outputDir)

  // Worker.setMasterAddr(masterIp, masterPort)
  // Worker.setInputDirs(inputDirs)
  // Worker.setOutputDir(outputDir)

  // val workerIp = SystemUtils.getLocalIp.getOrElse {
  //   println("Failed to get local IP address")
  //   sys.exit(1)
  // }
  
  // val ramMb = SystemUtils.getRamMb

  // // Create worker service to receive range assignments
  // val workerService = new WorkerServiceImpl()

  val server = ServerBuilder
    .forPort(8080)
    // .addService(WorkerServiceGrpc.bindService(workerService, ec))
    .addService(ShuffleGrpc.bindService(new ShuffleServiceImpl(), ec))
    .build()

  server.start()

  println(s"Worker server started on port ${server.getPort}")

  Thread.sleep(10000)

  println("Starting shuffle client...")

  val mockAssignedRange = Map(
    ("2.2.2.101", 8080) -> (ByteString.EMPTY, ByteString.EMPTY),
    ("2.2.2.102", 8080) -> (ByteString.EMPTY, ByteString.EMPTY),
    ("2.2.2.103", 8080) -> (ByteString.EMPTY, ByteString.EMPTY),
  )
  Worker.setAssignedRange(mockAssignedRange)

  val mockReceiverFileInfo = Map(
    "2.2.2.101" -> (1 to 320).map(i => s"partition.$i").toList,
    "2.2.2.102" -> (1 to 320).map(i => s"partition.$i").toList,
    "2.2.2.103" -> (1 to 320).map(i => s"partition.$i").toList,
  )
  val start = System.nanoTime()

  // if (SystemUtils.getLocalIp.getOrElse("") == "2.2.2.101")
  new ShuffleClient().start(mockReceiverFileInfo).andThen( {
    case _ =>
      val end = System.nanoTime()
      val durationSeconds = (end - start).toDouble / 1e9
      println(f"Shuffle completed in $durationSeconds%.2f seconds")
  })

  // val port = server.getPort

  // // Register with master
  // val client = new MasterClient(masterIp, masterPort)
  // client.registerWorker(workerIp, port, ramMb)

  // // Start sampling in a separate thread
  // val samplingPhase = Future {
  //   try {
  //     val samples = SamplingUtils.sampleFromInputs(inputDirs).getOrElse {
  //       println("Warning: Sampling failed")
  //       sys.exit(1)
  //     }
      
  //     val success = client.sampling(workerIp, samples)
      
  //     if (success) {
  //       println("Samples sent successfully. Waiting for range assignment...")
  //     } else {
  //       println("Failed to send samples to master")
  //     }
  //   } catch {
  //     case e: Exception =>
  //       println(s"Error during sampling: ${e.getMessage}")
  //       e.printStackTrace()
  //   }
  // }

  server.awaitTermination()
}
