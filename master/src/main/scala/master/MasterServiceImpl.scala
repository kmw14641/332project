package master

import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future}
import master.MasterService.{MasterServiceGrpc, WorkerInfo, RegisterWorkerResponse, SampleData, SampleResponse}
import master.Master

// TODO: handling fault tolerance
// 1. worker shutdown after Master.registerWorker
// 2. worker shutdown after return the registerWorker
// => then in sampling phase, 1 or 2 must be delayed so
// 1. worker gives registerWorker again => just update
// 2. worker gives samples => register worker works well, dont care about it
class MasterServiceImpl(implicit ec: ExecutionContext) extends MasterServiceGrpc.MasterService {
  override def registerWorker(request: WorkerInfo): Future[RegisterWorkerResponse] = {
    Master.registerWorker(request)
    if (Master.getRegisteredWorkers.size == Master.getWorkersNum) {
      println(Master.getRegisteredWorkers.keys.mkString(", "))
    }

    Future.successful(
      RegisterWorkerResponse(success = true)
    )
  }

  override def sampling(request: SampleData): Future[SampleResponse] = {
    val keys = request.keys
    val success = Master.addSamples(request.workerIp, keys)

    // If ranges are ready, trigger range assignment
    if (Master.isRangesReady) {
      // Spawn a separate thread to assign ranges to workers
      Future {
        Thread.sleep(500)  // Small delay to ensure all responses are sent
        assignRangesToWorkers()
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

    // TODO: use multithreading to speed up
    workers.zipWithIndex.foreach { case ((workerIp, workerInfo), idx) =>
      if (idx < ranges.length) {
        val (rangeStart, rangeEnd) = ranges(idx)
        
        // Create client to send range to worker
        val workerClient = new worker.WorkerClient(workerInfo.ip, workerInfo.port)
        try {
          workerClient.sampled(rangeStart, rangeEnd)
        } catch {
          case e: Exception =>
            println(s"Failed to assign range to worker $workerIp:${workerInfo.port}: ${e.getMessage}")
        }
      }
    }
  }
}
