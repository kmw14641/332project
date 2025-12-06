package server

import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext
import worker.WorkerService.SampleServiceGrpc
import worker.WorkerService.TerminationServiceGrpc
import scala.concurrent.Future
import state.TerminationState
import worker.WorkerService.TerminateAck
import worker.WorkerService.TerminateCommand

class TerminationServiceImpl(implicit ec: ExecutionContext) extends TerminationServiceGrpc.TerminationService {
  private val logger = LoggerFactory.getLogger(getClass)

  override def terminate(request: TerminateCommand): Future[TerminateAck] = Future {
    TerminationState.markTerminated()
    TerminateAck(success = true)
  }
}