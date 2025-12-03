package server

import scala.concurrent.{ExecutionContext, Future}
import master.MasterService.{SyncPhaseReport, SyncPhaseAck, SyncAndShuffleServiceGrpc}
import global.{MasterState, ConnectionManager}
import worker.WorkerService.{WorkerServiceGrpc, StartShuffleCommand}
import scala.async.Async.{async, await}
import common.utils.RetryUtils.retry

class SyncAndShuffleServiceImpl(implicit ec: ExecutionContext) extends SyncAndShuffleServiceGrpc.SyncAndShuffleService {
  override def reportSyncCompletion(request: SyncPhaseReport): Future[SyncPhaseAck] = {
    val (allCompleted, current, total) = MasterState.markSyncCompleted(request.workerIp)
    println(s"Worker ${request.workerIp} completed synchronization ($current/$total)")

    if (allCompleted && !MasterState.hasShuffleStarted) {
      Future {
        startShufflePhase()
      }
    }

    Future.successful(SyncPhaseAck(success = true))
  }

  private def startShufflePhase(): Unit = this.synchronized {
    if (MasterState.hasShuffleStarted) {
      println("Shuffle phase already started; ignoring duplicate request.")
      return
    }

    MasterState.markShuffleStarted()
    println("All workers reported sync completion. Triggering shuffle phase...")

    val workers = MasterState.getRegisteredWorkers
    workers.foreach { case (ip, info) =>
      retry {
        async {
          val stub = WorkerServiceGrpc.stub(ConnectionManager.getWorkerChannel(ip))
          val request = StartShuffleCommand(reason = "Shuffle phase start")
          await { stub.startShuffle(request) }
        }
      }
    }
  }
}
