package worker.sync

import master.MasterClient
import worker.Worker
import worker.WorkerService.{FileMetadata, WorkerNetworkInfo}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object SynchronizationManager {
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
    val selfInfo = Worker.getWorkerNetworkInfo.getOrElse {
      println("[Sync] Worker network information is unavailable. Abort synchronization.")
      return
    }

    val outgoingPlans = getOutgoingPlans(selfInfo)
    transmitPlans(outgoingPlans, selfInfo)
    notifyMasterOfCompletion(selfInfo._1)

    println("[Sync] Synchronization completed. Waiting for master's shuffle command...")

    Await.result(Worker.waitForShuffleCommand, Duration.Inf)

    println("[Sync] Master authorized shuffle phase. Ready for file transfers.")
    // Shuffle phase will starts after this point.
  }

  /*
  Consume worker-provided assignments and drop entries that point back to the current worker or are empty.
  */
  private def getOutgoingPlans(selfInfo: (String, Int)): Map[(String, Int), Seq[String]] = {
    Worker.getAssignedFiles.collect {
        case (endpoint, files) if endpoint != selfInfo && files.nonEmpty =>
          endpoint -> files.toSeq
      }
  }

  /*
  Transmit destination-specific transfer plans to actual gRPC calls.
  For each worker, create a PeerWorkerClient and send the file metadata list.
  Await all transmissions to complete before returning.
  */
  private def transmitPlans(
    plans: Map[(String, Int), Seq[String]],
    selfInfo: (String, Int)
  )(implicit ec: ExecutionContext): Unit = {
    if (plans.isEmpty) {
      println("[Sync] No outgoing files to report.")
      return
    }

    val senderInfo = WorkerNetworkInfo(ip = selfInfo._1, port = selfInfo._2)

    val sendFutures = plans.toSeq.map { case ((ip, port), files) =>
      val client = new PeerWorkerClient(ip, port)
      val metadata = files.map(fileName => FileMetadata(fileName = fileName))
      val fileNames = files.mkString(", ")
      println(s"[Sync][SendList] ${selfInfo._1}:${selfInfo._2} -> $ip:$port files: [$fileNames]")

      client.deliverFileList(senderInfo, metadata).map { success =>
        if (success) {
          println(s"[Sync] Delivered ${files.size} file descriptors to $ip:$port")
        } else {
          println(s"[Sync] Failed to deliver file descriptors to $ip:$port")
        }
        client.shutdown()
      }.recover { case e =>
        println(s"[Sync] Error delivering file descriptors to $ip:$port: ${e.getMessage}")
        client.shutdown()
      }
    }

    Await.result(Future.sequence(sendFutures), Duration.Inf)
  }

  private def notifyMasterOfCompletion(workerIp: String)(implicit ec: ExecutionContext): Unit = {
  Worker.getMasterAddr match {
    case Some((masterIp, masterPort)) =>
      val client = new MasterClient(masterIp, masterPort)

      client.reportSyncCompletion(workerIp).onComplete {
        case scala.util.Success(true) =>
          println("[Sync] Reported synchronization completion to master.")
          client.shutdown()
        case scala.util.Success(false) =>
          println("[Sync] Master rejected synchronization completion report.")
          client.shutdown()
        case scala.util.Failure(e) =>
          println(s"[Sync] Error reporting synchronization completion: ${e.getMessage}")
          client.shutdown()
      }

    case None =>
      println("[Sync] Master address is unknown. Unable to report synchronization completion.")
    }
  } 
}
