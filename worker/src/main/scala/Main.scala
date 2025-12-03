import java.nio.file.{Files, Paths}

import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._
import scala.async.Async.{async, await}

import io.grpc.ServerBuilder

import common.utils.SystemUtils
import global.WorkerState
import global.ConnectionManager
import server.{WorkerServiceImpl, ShuffleServiceImpl}
import worker.WorkerService.WorkerServiceGrpc
import main.{RegisterManager, SampleManager, MemorySortManager, FileMergeManager, LabelingManager, SynchronizationManager, ShuffleManager, TerminationManager}
import utils.WorkerOptionUtils
import utils.FileManager
import shuffle.Shuffle.ShuffleGrpc
import global.StateRestoreManager
import scala.concurrent.Future
import state.SampleState

object Main extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global

  val (masterAddr, inputDirs, outputDir) = WorkerOptionUtils.parse(args).getOrElse {
    sys.exit(1)
  }
  
  val (masterIp, masterPort) = {
    val parts = masterAddr.split(":")
    (parts(0), parts(1).toInt)
  }

  val invalidInputDirs = inputDirs.filter{ dir => !Files.exists(Paths.get(dir)) || !Files.isDirectory(Paths.get(dir)) }

  if (invalidInputDirs.nonEmpty) {
    invalidInputDirs.foreach { dir =>
      Console.err.println(s"Input directory does not exist or is not a directory: $dir")
    }
    sys.exit(1)
  }

  FileManager.createDirectoryIfNotExists(outputDir)

  ConnectionManager.initMasterChannel(masterIp, masterPort)
  FileManager.setInputDirs(inputDirs)
  FileManager.setOutputDir(outputDir)

  if (!StateRestoreManager.isClean()) {
    StateRestoreManager.restoreState()
  }

  val server = ServerBuilder
    .forPort(0)
    .maxInboundMessageSize(ConnectionManager.maxGrpcMessageSize)
    .addService(WorkerServiceGrpc.bindService(new WorkerServiceImpl(), ec))
    .addService(ShuffleGrpc.bindService(new ShuffleServiceImpl(FileManager.labelingDirName), ec))
    .build()

  server.start()

  val mainWaiting = async {
    val masterFuture = async {
      await { new RegisterManager().start(server.getPort) }

      await { new SampleManager().start() }

      await { SampleState.waitForAssignment() }
    }

    val localFuture = async {
      val files = await { new MemorySortManager(FileManager.memSortDirName).start }

      val sortedFiles = await { new FileMergeManager(FileManager.memSortDirName, FileManager.fileMergeDirName).start(files) }

      sortedFiles
    }

    val (_, sortedFiles) = await { masterFuture.zip(localFuture) }

    val assignedRange = SampleState.getAssignedRange.getOrElse(throw new RuntimeException("Assigned range is not available"))
    ConnectionManager.initWorkerChannels(assignedRange.keys.toSeq)
    val labeledFiles = await { new LabelingManager(FileManager.fileMergeDirName, FileManager.labelingDirName, assignedRange).start(sortedFiles) }

    labeledFiles.foreach {
      case (workerId, fileList) =>
        val fileNames = fileList.mkString(", ")
        println(s"[Labeling][Assigned] ${workerId._1}:${workerId._2} files: [$fileNames]")
    }

    val shufflePlans =  await { new SynchronizationManager(labeledFiles).start() }

    shufflePlans.foreach {
      case (workerIp, fileList) =>
        val fileNames = fileList.mkString(", ")
        println(s"[Shuffle][Planned] $workerIp files: [$fileNames]")
    }

    val completedShufflePlans = await { new ShuffleManager(FileManager.labelingDirName, FileManager.shuffleDirName).start(shufflePlans) }

    println(s"[Shuffle][Completed] files: [${completedShufflePlans.mkString(", ")}]")

    val finalFiles = await { new FileMergeManager(FileManager.shuffleDirName, FileManager.finalDirName).start(completedShufflePlans) }
    FileManager.mergeAllFiles(s"$outputDir/sorted.bin", finalFiles, FileManager.finalDirName)
    println(s"[Completed] Final output file: ${s"$outputDir/sorted.bin"}")

    await { new TerminationManager().shutdownServerSafely(server) }

    FileManager.deleteAll(finalFiles)
    FileManager.deleteAllSubDir

    StateRestoreManager.clear()
  }

  Await.result(mainWaiting, Duration.Inf)
}
