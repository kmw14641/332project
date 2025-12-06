package server

import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext
import global.ConnectionManager
import scala.concurrent.Future
import worker.WorkerService.IntroduceAck
import worker.WorkerService.WorkerNetworkInfo
import worker.WorkerService.SampleServiceGrpc
import global.StateRestoreManager
import state.SampleState
import worker.WorkerService.AssignRangesResponse
import worker.WorkerService.WorkersRangeAssignment
import io.grpc.StatusException
import io.grpc.Status
import worker.WorkerService.RangeAssignment
import java.math.BigInteger

class SampleServiceImpl(implicit ec: ExecutionContext) extends SampleServiceGrpc.SampleService {
  private val logger = LoggerFactory.getLogger(getClass)

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
    SampleState.setAssignedRange(workersRangeAssignment)
    StateRestoreManager.storeState()
    SampleState.markAssigned()

    workersRangeAssignment.foreach {
      // Print assigned ranges for debugging
      case ((ip, port), (start, end)) =>
        val startInt = new BigInteger(1, start.toByteArray())
        val endInt = new BigInteger(1, end.toByteArray())
        logger.info(s"Assigned range to worker $ip:$port => [${startInt.toString(16)}, ${endInt.toString(16)})")
    }

    val response = Future.successful(
      AssignRangesResponse(success = true)
    )

    response
  }
}