package main

import org.slf4j.LoggerFactory
import common.utils.SystemUtils
import global.{ConnectionManager, WorkerState}
import master.MasterService.{SyncAndShuffleServiceGrpc, SyncPhaseReport}
import scala.async.Async.{async, await}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import worker.WorkerService.{FileListMessage, WorkerServiceGrpc}
import state.LabelingState
import state.SynchronizationState
import global.StateRestoreManager
import common.utils.RetryUtils.retry

class SynchronizationManager(labeledFiles: Map[(String, Int), List[String]])(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)

  labeledFiles.foreach {
    case ((ip, port), files) =>
      val fileNames = files.mkString(", ")
      logger.info(s"[Sync][Assigned] $ip:$port files: [$fileNames]")
  }
  
  LabelingState.setAssignedFiles(labeledFiles)  // TODO: do at labeling phase
  private val masterStub = SyncAndShuffleServiceGrpc.stub(ConnectionManager.getMasterChannel())

  def start(): Future[Map[String, Seq[String]]] = {
    val selfIp = SystemUtils.getLocalIp.getOrElse(
      throw new IllegalStateException("[Sync] Failed to determine local IP. Abort synchronization.")
    )

    async {
      addLocalPlan(selfIp)
      val outgoingPlans = getOutgoingPlans(selfIp)
      await(transmitPlans(outgoingPlans, selfIp))
      await(notifyMasterOfCompletion(selfIp))
      await(SynchronizationState.waitForShuffleCommand)

      logger.info("[Sync] Master authorized shuffle phase. Ready for file transfers.")

      SynchronizationState.getShufflePlans
    }
  }

  private def addLocalPlan(selfIp: String): Unit = {
    val localFiles = LabelingState.getAssignedFiles
      .find { case ((ip, _), _) => ip == selfIp }
      .map(_._2)
      .getOrElse(Nil)
    SynchronizationState.setShufflePlan(selfIp, localFiles)
  }

  /*
  Consume worker-provided assignments and drop entries that point back to the current worker or are empty.
  */
  private def getOutgoingPlans(selfIp: String): Map[(String, Int), Seq[String]] = {
    LabelingState.getAssignedFiles.foreach {
      case ((ip, port), files) =>
        logger.info(s"[Sync] Preparing outgoing plan to $ip:$port with ${files.mkString(", ")} files.")
    }
      LabelingState.getAssignedFiles.collect {
        case (endpoint, files) if endpoint._1 != selfIp && files.nonEmpty =>
          endpoint -> files
      }
  }

  /*
  Transmit destination-specific transfer plans to actual gRPC calls.
  For each worker, send the file metadata list via gRPC.
  Await all transmissions to complete before returning.
  */
  private def transmitPlans(plans: Map[(String, Int), Seq[String]], selfIp: String): Future[Unit] = async {
    if (SynchronizationState.isTransmitCompleted) {
      logger.info("[StateRestore] Skip transmitPlans")
      ()
    } else if (plans.isEmpty) {
      logger.info("[Sync] No outgoing files to report.")
      ()
    } 
    else {
      val sendFutures = plans.toSeq.map { case ((ip, port), files) =>
        retry {
          async {
            val fileNames = files.mkString(", ")
            logger.info(s"[Sync][SendList] $selfIp -> $ip:$port files: [$fileNames]")

            val stub = WorkerServiceGrpc.stub(ConnectionManager.getWorkerChannel(ip))
            val request = FileListMessage(
              senderIp = selfIp,
              files = files
            )

            await { stub.deliverFileList(request) }
            logger.info(s"[Sync] Delivered ${files.size} file descriptors to $ip:$port")
          }
        }
      }

      await { Future.sequence(sendFutures) }

      SynchronizationState.completeTransmit()
      StateRestoreManager.storeState()
    }
  }

  private def notifyMasterOfCompletion(workerIp: String): Future[Unit] = async {
    if (SynchronizationState.isReportCompleted) {
      logger.info("[StateRestore] Skip synchronization report")
      ()
    }
    else {
      val request = SyncPhaseReport(workerIp = workerIp)
      await {masterStub.reportSyncCompletion(request).andThen {
        case Success(_) =>
          logger.info("[Sync] Synchronization completed. Waiting for master's shuffle command...")
          SynchronizationState.completeReport()
          StateRestoreManager.storeState()
        case Failure(e) =>
          logger.error(s"[Sync] Failed to report synchronization completion: ${e.getMessage}")
        }
      }
    }
  }
}
