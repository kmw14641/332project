package server

import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.async.Async.{async, await}
import master.MasterService.{SampleData, SampleResponse, SamplingServiceGrpc}
import global.{MasterState, ConnectionManager}
import worker.WorkerService.{SampleServiceGrpc, WorkersRangeAssignment, WorkerRangeAssignment, WorkerNetworkInfo, RangeAssignment}
import common.utils.RetryUtils.retry

class SamplingServiceImpl(implicit ec: ExecutionContext) extends SamplingServiceGrpc.SamplingService {
  private val logger = LoggerFactory.getLogger(getClass)
  
  override def sampling(request: SampleData): Future[SampleResponse] = {
    val keys = request.keys
    val success = MasterState.addSamples(request.workerIp, keys)

    // If all workers have sent samples, spawn a thread to calculate ranges and assign them
    if (MasterState.getSampleSize == MasterState.getWorkersNum && success && MasterState.tryStartCalculateRanges()) {
      Future {
        MasterState.calculateRanges()
        if (MasterState.isRangesReady) {
          assignRangesToWorkers()
        }
      }
    }

    Future.successful(
      SampleResponse(success = success)
    )
  }

  private def assignRangesToWorkers(): Unit = {
    val workers = MasterState.getRegisteredWorkers.toSeq.sortBy(_._1)
    val ranges = MasterState.getRanges

    logger.info("Assigning ranges to workers...")
    
    val request = WorkersRangeAssignment(
      assignments = ranges.map { case ((workerIp, workerPort), (start, end)) =>
        WorkerRangeAssignment(
          worker = Some(WorkerNetworkInfo(ip = workerIp, port = workerPort)),
          range = Some(RangeAssignment(start = start, end = end))
        )
      }.toSeq
    )
    
    for ((ip, info) <- workers) {
      retry {
        async {
          val stub = SampleServiceGrpc.stub(ConnectionManager.getWorkerChannel(ip))
          await { stub.assignRanges(request) }
        }
      }
    }
  }
}
