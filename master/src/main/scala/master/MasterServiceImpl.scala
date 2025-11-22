package master

import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future}
import master.MasterService.{MasterServiceGrpc, WorkerInfo, RegisterWorkerResponse, SampleData, SampleResponse, SyncPhaseReport, SyncPhaseAck}
import master.Master
import worker.WorkerClient

// TODO: handling fault tolerance
// 1. worker shutdown after Master.registerWorker
// 2. worker shutdown after return the registerWorker
// => then in sampling phase, 1 or 2 must be delayed so
// 1. worker gives registerWorker again => just update
// 2. worker gives samples => register worker works well, dont care about it
class MasterServiceImpl(implicit ec: ExecutionContext) extends MasterServiceGrpc.MasterService {
  override def registerWorker(request: WorkerInfo): Future[RegisterWorkerResponse] = {
    Master.registerWorker(request)

    Future.successful(
      RegisterWorkerResponse(success = true)
    )
  }

  override def sampling(request: SampleData): Future[SampleResponse] = {
    val keys = request.keys
    val success = Master.addSamples(request.workerIp, keys)

    // If all workers have sent samples, calculate ranges
    if (Master.getSampleSize == Master.getWorkersNum && success) {
      Master.calculateRanges()
      // If ranges are ready, trigger range assignment
      if (Master.isRangesReady) {
        // Spawn a separate thread to assign ranges to workers
        Future {
          assignRangesToWorkers()
        }
      }
    }


    Future.successful(
      SampleResponse(success = success)
    )
  }

  private def assignRangesToWorkers(): Unit = {
    val workers = Master.getRegisteredWorkers.toSeq.sortBy(_._1)  // Sort by IP for consistent ordering
    val ranges = Master.getRanges

    println("Assigning ranges to workers...")

    for (
      (ip, info) <- workers
    ) {
      val assign = Future {
        val workerClient = new WorkerClient(ip, info.port)
        workerClient.assignRanges(ranges)
      }
      assign.recover {
        case e: Exception =>
          println(s"Failed to assign range to worker $ip:${info.port}: ${e.getMessage}")
      }
    }
  }
  /*
  By using markSyncCompleted, check if all workers have reported sync completion.
  If all have completed and shuffle has not started yet, trigger the shuffle phase.
  */
  override def reportSyncCompletion(request: SyncPhaseReport): Future[SyncPhaseAck] = {
    val (allCompleted, current, total) = Master.markSyncCompleted(request.workerIp)
    println(s"Worker ${request.workerIp} completed synchronization ($current/$total)")

    if (allCompleted && !Master.hasShuffleStarted) {
      Future {
        startShufflePhase()
      }
    }

    Future.successful(SyncPhaseAck(success = true))
  }

  private def startShufflePhase(): Unit = this.synchronized {
    if (Master.hasShuffleStarted) {
      println("Shuffle phase already started; ignoring duplicate request.")
      return
    }

    Master.markShuffleStarted()
    println("All workers reported sync completion. Triggering shuffle phase...")

    val workers = Master.getRegisteredWorkers
    workers.foreach {
    case (ip, info) =>
      val workerClient = new WorkerClient(ip, info.port)
      workerClient.startShuffle().recover {
        case e: Exception =>
          println(s"Failed to send shuffle start command to worker $ip:${info.port}: ${e.getMessage}")
          false
      }.onComplete(_ => workerClient.shutdown())
    }
  }
}
