package server

import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Future}
import master.MasterService.{SyncPhaseReport, SyncPhaseAck}
import master.MasterService
import worker.WorkerService
import global.{MasterState, ConnectionManager}
import worker.WorkerService.StartShuffleCommand
import scala.async.Async.{async, await}
import common.utils.RetryUtils.retry

class SyncServiceImpl(implicit ec: ExecutionContext) extends MasterService.SyncServiceGrpc.SyncService {
  private val logger = LoggerFactory.getLogger(getClass)
  
  override def reportSyncCompletion(request: SyncPhaseReport): Future[SyncPhaseAck] = {
    val (allCompleted, current, total) = MasterState.markSyncCompleted(request.workerIp)
    logger.info(s"Worker ${request.workerIp} completed synchronization ($current/$total)")

    if (allCompleted && !MasterState.hasShuffleStarted) {
      Future {
        startShufflePhase()
      }
    }

    Future.successful(SyncPhaseAck(success = true))
  }

  private def startShufflePhase(): Unit = this.synchronized {
    if (MasterState.hasShuffleStarted) {
      logger.info("Shuffle phase already started; ignoring duplicate request.")
      return
    }

    MasterState.markShuffleStarted()
    logger.info("All workers reported sync completion. Triggering shuffle phase...")

    val workers = MasterState.getRegisteredWorkers
    workers.foreach { case (ip, info) =>
      retry {
        async {
          val stub = WorkerService.SyncServiceGrpc.stub(ConnectionManager.getWorkerChannel(ip))
          val request = StartShuffleCommand(reason = "Shuffle phase start")
          await { stub.startShuffle(request) }
        }
      }
    }
  }
}
