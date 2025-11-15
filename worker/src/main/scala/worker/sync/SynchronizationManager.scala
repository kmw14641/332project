package worker.sync

import master.MasterClient
import utils.PathUtils
import worker.Worker
import worker.WorkerService.{FileMetadata, WorkerNetworkInfo}

import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object SynchronizationManager {
  private val FileNamePattern = """^(.+?)_(.+?)_(\d+)$""".r

  def triggerSyncPhase()(implicit ec: ExecutionContext): Unit = {
    Future {
      runSyncPhase()
    }.recover {
      case e: Exception =>
        println(s"[Sync] Synchronization phase failed: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  private def runSyncPhase()(implicit ec: ExecutionContext): Unit = {
    val labeledRoot = Paths.get("/labeled")
    if (!Files.exists(labeledRoot) || !Files.isDirectory(labeledRoot)) {
      println("[Sync] Labeled directory '/labeled' is missing. Abort synchronization.")
      return
    }
    val outputDir = labeledRoot.toAbsolutePath.toString

    val selfInfo = Worker.getWorkerNetworkInfo.getOrElse {
      println("[Sync] Worker network information is unavailable. Abort synchronization.")
      return
    }
    val assigned = Worker.getAssignedRange.getOrElse {
      println("[Sync] Assigned ranges are missing. Abort synchronization.")
      return
    }

    val workerLookupByIp: Map[String, (String, Int)] =
      assigned.keys.groupBy(_._1).view.mapValues(_.head).toMap

    val outgoingPlans = collectOutgoingPlans(outputDir, selfInfo._1, workerLookupByIp)
    transmitPlans(outgoingPlans, selfInfo)
    notifyMasterOfCompletion(selfInfo._1)

    println("[Sync] Synchronization completed. Waiting for master's shuffle command...")

    Await.result(Worker.waitForShuffleCommand, Duration.Inf)

    println("[Sync] Master authorized shuffle phase. Ready for file transfers.")
    // Shuffle phase will starts after this point.
  }
  
  //Indicates which file to send to whom.
  private case class LocalFileDescriptor(fileName: String, destinationIp: String)

  /*
  By using labeled directory, scan all files and prepare plans for outgoing file metadata.
  Parse the file name and group files by their destination worker.
  Return a map where the key is the worker's (IP, port) and the value is a sequence of LocalFileDescriptor.
  */
  private def collectOutgoingPlans(
    outputDir: String,
    selfIp: String,
    lookup: Map[String, (String, Int)]
  ): Map[(String, Int), Seq[LocalFileDescriptor]] = {
    val files = PathUtils.getFilesList(outputDir)
    val plans = mutable.Map[(String, Int), mutable.ListBuffer[LocalFileDescriptor]]()

    files.foreach { filePath =>
      val path = Paths.get(filePath)
      if (Files.isRegularFile(path)) {
        val fileName = path.getFileName.toString
        fileName match {
          case FileNamePattern(_, toIp, _) if toIp == selfIp =>
            // File is destined for this worker. No need to send metadata elsewhere.
          case FileNamePattern(_, toIp, _) =>
            lookup.get(toIp) match {
              case Some(endpoint) =>
                val descriptor = LocalFileDescriptor(fileName, toIp)
                val buffer = plans.getOrElseUpdate(endpoint, mutable.ListBuffer.empty)
                buffer += descriptor
              case None =>
                println(s"[Sync] No registered worker for destination IP $toIp (file: $fileName). Skipping.")
            }
          case _ =>
            println(s"[Sync] File $fileName does not match expected pattern. Skipping.")
        }
      }
    }

    plans.view.mapValues(_.toSeq).toMap
  }

  /*
  Transmit destination-specific transfer plans to actual gRPC calls.
  For each worker, create a PeerWorkerClient and send the file metadata list.
  Await all transmissions to complete before returning.
  */
  private def transmitPlans(
    plans: Map[(String, Int), Seq[LocalFileDescriptor]],
    selfInfo: (String, Int)
  )(implicit ec: ExecutionContext): Unit = {
    if (plans.isEmpty) {
      println("[Sync] No outgoing files to report.")
      return
    }

    val senderInfo = WorkerNetworkInfo(ip = selfInfo._1, port = selfInfo._2)

    val sendFutures = plans.toSeq.map { case ((ip, port), files) =>
      Future {
        val client = new PeerWorkerClient(ip, port)
        try {
          val metadata = files.map(f => FileMetadata(fileName = f.fileName))
          //'fileNames' stores the list of file to be transmitted.
          val fileNames = files.map(_.fileName).mkString(", ")
          println(s"[Sync][SendList] ${selfInfo._1}:${selfInfo._2} -> $ip:$port files: [$fileNames]")
          
          // Interchanging file list occurs here.
          val success = client.deliverFileList(senderInfo, metadata)
          if (success) {
            println(s"[Sync] Delivered ${files.size} file descriptors to $ip:$port")
          } else {
            println(s"[Sync] Failed to deliver file descriptors to $ip:$port")
          }
        } finally {
          client.shutdown()
        }
      }
    }

    Await.result(Future.sequence(sendFutures), Duration.Inf)
  }

  private def notifyMasterOfCompletion(workerIp: String)(implicit ec: ExecutionContext): Unit = {
    Worker.getMasterAddr match {
      case Some((masterIp, masterPort)) =>
        val client = new MasterClient(masterIp, masterPort)
        try {
          val success = client.reportSyncCompletion(workerIp)
          if (success) {
            println("[Sync] Reported synchronization completion to master.")
          } else {
            println("[Sync] Master rejected synchronization completion report.")
          }
        } finally {
          client.shutdown()
        }
      case None =>
        println("[Sync] Master address is unknown. Unable to report synchronization completion.")
    }
  }
}
