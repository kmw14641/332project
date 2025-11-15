package server

import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future}
import master.MasterService.{MasterServiceGrpc, WorkerInfo, RegisterWorkerResponse, SampleData, SampleResponse}
import global.MasterState
import client.WorkerClient

// TODO: handling fault tolerance
// 1. worker shutdown after Master.registerWorker
// 2. worker shutdown after return the registerWorker
// => then in sampling phase, 1 or 2 must be delayed so
// 1. worker gives registerWorker again => just update
// 2. worker gives samples => register worker works well, dont care about it
class MasterServiceImpl(implicit ec: ExecutionContext) extends MasterServiceGrpc.MasterService {
  override def registerWorker(request: WorkerInfo): Future[RegisterWorkerResponse] = {
    MasterState.registerWorker(request)

    Future.successful(
      RegisterWorkerResponse(success = true)
    )
  }

  override def sampling(request: SampleData): Future[SampleResponse] = {
    val keys = request.keys
    val success = MasterState.addSamples(request.workerIp, keys)

    // If all workers have sent samples, calculate ranges
    if (MasterState.getSampleSize == MasterState.getWorkersNum && success) {
      MasterState.calculateRanges()
      // If ranges are ready, trigger range assignment
      if (MasterState.isRangesReady) {
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
    val workers = MasterState.getRegisteredWorkers.toSeq.sortBy(_._1)  // Sort by IP for consistent ordering
    val ranges = MasterState.getRanges

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
}
