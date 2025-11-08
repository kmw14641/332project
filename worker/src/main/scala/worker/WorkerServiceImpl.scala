package worker

import scala.concurrent.{ExecutionContext, Future}
import worker.WorkerService.{WorkerServiceGrpc, RangeAssignment, RangeResponse}

class WorkerServiceImpl(implicit ec: ExecutionContext) extends WorkerServiceGrpc.WorkerService {
  override def sampled(request: RangeAssignment): Future[RangeResponse] = {
    val rangeStart = request.rangeStart
    val rangeEnd = request.rangeEnd

    // Store the assigned range in the Worker singleton
    Worker.setAssignedRange(rangeStart, rangeEnd)

    Future.successful(
      RangeResponse(success = true)
    )
  }
}
