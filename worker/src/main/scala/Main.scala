import io.grpc.ServerBuilder
import scala.concurrent.{ExecutionContext, Future, Await, Promise}
import scala.concurrent.duration._
import utils.{WorkerOptionUtils, PathUtils, SamplingUtils, MergeSortUtils}
import master.MasterClient
import worker.{Worker, WorkerServiceImpl}
import worker.WorkerService.WorkerServiceGrpc
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

  println("Starting sampling and merge sort phases in parallel...")

  // Start sampling in a separate thread
  val samplingPhase = Future {
    try {
      println("[Sampling] Starting sampling phase...")
      val samples = SamplingUtils.sampleFromInputs(inputDirs).getOrElse {
        println("[Sampling] Warning: Sampling failed")
        sys.exit(1)
      }
      
      val success = client.sampling(workerIp, samples)
      
      if (success) {
        println("[Sampling] Samples sent successfully. Waiting for range assignment...")
      } else {
        println("[Sampling] Failed to send samples to master")
      }
    } catch {
      case e: Exception =>
        println(s"[Sampling] Error during sampling: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  // Prepare intermediate directory for merge sort (different from outputDir)
  val intermediateDir = "/intermediate"
  PathUtils.createDirectoryIfNotExists(intermediateDir)

  // Variable to store sorted files
  var sortedFiles: List[String] = List.empty

  // Start merge sort in a separate thread
  val mergeSortPhase = Future {
    try {
      println("[MergeSort] Starting disk-based merge sort...")
      sortedFiles = MergeSortUtils.diskBasedMergeSort(inputDirs, intermediateDir)
      println("[MergeSort] Disk-based merge sort completed successfully")
    } catch {
      case e: Exception =>
        println(s"[MergeSort] Error during merge sort: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  // Wait for both phases to complete
  val bothPhases = Future.sequence(Seq(samplingPhase, mergeSortPhase))
  
  try {
    Await.result(bothPhases, Duration.Inf)
    println("Both sampling and merge sort phases completed successfully")
  } catch {
    case e: Exception =>
      println(s"Error waiting for phases to complete: ${e.getMessage}")
      e.printStackTrace()
  }

  // Debug: Validate sorted output using valsort
  if (sortedFiles.nonEmpty) {
    try {
      println(s"[Validation] Merging ${sortedFiles.size} sorted files into single file for validation...")
      
      // Concatenate all sorted files into one
      val validationFile = s"$intermediateDir/validation_output.bin"
      val validationPath = java.nio.file.Paths.get(validationFile)
      
      // Create output file
      val outputChannel = java.nio.channels.FileChannel.open(
        validationPath,
        java.nio.file.StandardOpenOption.CREATE,
        java.nio.file.StandardOpenOption.WRITE,
        java.nio.file.StandardOpenOption.TRUNCATE_EXISTING
      )
      
      try {
        // Copy all sorted files in order
        sortedFiles.foreach { filePath =>
          val inputPath = java.nio.file.Paths.get(filePath)
          val inputChannel = java.nio.channels.FileChannel.open(
            inputPath,
            java.nio.file.StandardOpenOption.READ
          )
          try {
            inputChannel.transferTo(0, java.nio.file.Files.size(inputPath), outputChannel)
          } finally {
            inputChannel.close()
          }
        }
      } finally {
        outputChannel.close()
      }
      
      println(s"[Validation] Created validation file: $validationFile")
      println(s"[Validation] Running valsort validation...")
      
      // Run valsort command
      import scala.sys.process._
      val valsortResult = try {
        val output = s"valsort $validationFile".!!
        println(s"[Validation] valsort output:\n$output")
        println("[Validation] ✓ Validation successful!")
      } catch {
        case e: Exception =>
          println(s"[Validation] ✗ Validation failed: ${e.getMessage}")
      }
      
    } catch {
      case e: Exception =>
        println(s"[Validation] Error during validation: ${e.getMessage}")
        e.printStackTrace()
    }
  } else {
    println("[Validation] No sorted files to validate")
  }

  server.awaitTermination()
}