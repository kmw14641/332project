package server

import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext
import worker.WorkerService.RegisterServiceGrpc
import global.ConnectionManager
import scala.concurrent.Future
import worker.WorkerService.IntroduceAck
import worker.WorkerService.WorkerNetworkInfo

class RegisterServiceImpl(implicit ec: ExecutionContext) extends RegisterServiceGrpc.RegisterService {
  private val logger = LoggerFactory.getLogger(getClass)

  override def introduceNewWorker(request: WorkerNetworkInfo): Future[IntroduceAck] = Future {
    ConnectionManager.setWorkerChannel(request.ip, request.port)
    IntroduceAck(success = true)
  }
}