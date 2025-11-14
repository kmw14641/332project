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
    .maxInboundMessageSize(1024 * 1024 * 1024)
    // .addService(WorkerServiceGrpc.bindService(workerService, ec))
    .addService(ShuffleGrpc.bindService(new ShuffleServiceImpl(), ec))
    .build()

  server.start()

  println(s"Worker server started on port ${server.getPort}")

  Thread.sleep(10000)

  println("Starting shuffle client...")

  val mockAssignedRange = (101 to 110).map { i =>
    (s"2.2.2.$i", 8080) -> (ByteString.EMPTY, ByteString.EMPTY)
  }.toMap
  Worker.setAssignedRange(mockAssignedRange)

val (s, e) = SystemUtils.getLocalIp.getOrElse("") match {
  case "2.2.2.101" => (1, 10)
  case "2.2.2.102" => (11, 20)
  case "2.2.2.103" => (21, 30)
  case "2.2.2.104" => (31, 40)
  case "2.2.2.105" => (41, 50)
  case "2.2.2.106" => (51, 60)
  case "2.2.2.107" => (61, 70)
  case "2.2.2.108" => (71, 80)
  case "2.2.2.109" => (81, 90)
  case "2.2.2.110" => (91, 100)
  case _ => (0, 0) // default case for safety, but should never happen if IPs are as expected
}

  // 공통 파티션 리스트
  val partitions = (s to e).map(i => s"partition.$i").toList

  val mockReceiverFileInfo = Map(
    "2.2.2.101" -> partitions,
    "2.2.2.102" -> partitions,
    "2.2.2.103" -> partitions,
    "2.2.2.104" -> partitions,
    "2.2.2.105" -> partitions,
    "2.2.2.106" -> partitions,
    "2.2.2.107" -> partitions,
    "2.2.2.108" -> partitions,
    "2.2.2.109" -> partitions,
    "2.2.2.110" -> partitions
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
