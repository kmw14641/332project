package worker

import scala.concurrent.{ExecutionContext, Future}
import worker.WorkerService.{WorkerServiceGrpc, WorkersRangeAssignment, RangeAssignment, WorkerNetworkInfo, SampledResponse, WorkerRangeAssignment}
import io.grpc.Status

class WorkerServiceImpl(implicit ec: ExecutionContext) extends WorkerServiceGrpc.WorkerService {
  override def sampled(request: WorkersRangeAssignment): Future[SampledResponse] = {
    val workersRangeAssignment = request.assignments.map {
      case workerRange => {
        val workerInfo: WorkerNetworkInfo = workerRange.worker.getOrElse(
          throw Status.INVALID_ARGUMENT.withDescription("WorkerNetworkInfo is missing").asRuntimeException()
        )
        val rangeInfo: RangeAssignment = workerRange.range.getOrElse(
          throw Status.INVALID_ARGUMENT.withDescription("RangeAssignment is missing").asRuntimeException()
        )

        (workerInfo.ip, workerInfo.port) -> (rangeInfo.start, rangeInfo.end)
      }
    }.toMap

    // Store the assigned range in the Worker singleton
    Worker.setAssignedRange(workersRangeAssignment)

    Future.successful(
      SampledResponse(success = true)
    )
  }
}
