package worker

import scala.concurrent.{ExecutionContext, Future}
import worker.WorkerService.{WorkerServiceGrpc, WorkersRangeAssignment, RangeAssignment, WorkerNetworkInfo, SampledResponse, WorkerRangeAssignment}
import io.grpc.Status
import java.math.BigInteger

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
    workersRangeAssignment.map {
      // Print assigned ranges for debugging
      case ((ip, port), (start, end)) =>
        val startInt = new BigInteger(1, start.toByteArray())
        val endInt = new BigInteger(1, end.toByteArray())
        println(s"Assigned range to worker $ip:$port => [${startInt.toString(16)}, ${endInt.toString(16)})")
    }

    Future.successful(
      SampledResponse(success = true)
    )
  }
}
