package server

import scala.concurrent.{ExecutionContext, Future}
import worker.WorkerService.{WorkerServiceGrpc, WorkersRangeAssignment, RangeAssignment, WorkerNetworkInfo, AssignRangesResponse, WorkerRangeAssignment}
import io.grpc.{Status, StatusException}
import java.math.BigInteger
import global.Worker

class WorkerServiceImpl(implicit ec: ExecutionContext) extends WorkerServiceGrpc.WorkerService {
  override def assignRanges(request: WorkersRangeAssignment): Future[AssignRangesResponse] = {
    val workersRangeAssignment = request.assignments.map {
      case workerRange => {
        val workerInfo: WorkerNetworkInfo = workerRange.worker.getOrElse(
          throw new StatusException(Status.INVALID_ARGUMENT.withDescription("WorkerNetworkInfo is missing"))
        )
        val rangeInfo: RangeAssignment = workerRange.range.getOrElse(
          throw new StatusException(Status.INVALID_ARGUMENT.withDescription("RangeAssignment is missing"))
        )

        (workerInfo.ip, workerInfo.port) -> (rangeInfo.start, rangeInfo.end)
      }
    }.toMap

    // Store the assigned range in the Worker singleton
    Worker.setAssignedRange(workersRangeAssignment)
    workersRangeAssignment.foreach {
      // Print assigned ranges for debugging
      case ((ip, port), (start, end)) =>
        val startInt = new BigInteger(1, start.toByteArray())
        val endInt = new BigInteger(1, end.toByteArray())
        println(s"Assigned range to worker $ip:$port => [${startInt.toString(16)}, ${endInt.toString(16)})")
    }

    Future.successful(
      AssignRangesResponse(success = true)
    )
  }
}
