package server

import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Future}
import master.MasterService
import master.MasterService.{WorkerInfo, RegisterWorkerResponse}
import global.{MasterState, ConnectionManager}
import scala.async.Async.{async, await}
import worker.WorkerService
import worker.WorkerService.WorkerNetworkInfo

class RegisterServiceImpl(implicit ec: ExecutionContext) extends MasterService.RegisterServiceGrpc.RegisterService {
  private val logger = LoggerFactory.getLogger(getClass)

  override def registerWorker(request: WorkerInfo): Future[RegisterWorkerResponse] = Future {
    val faultOccured = MasterState.registerWorker(request)
    ConnectionManager.registerWorkerChannel(request.ip, request.port)
    if (faultOccured) {
      MasterState.getRegisteredWorkers.filter(_._1 != request.ip).map { case (workerIp, _) => async {
          val stub = WorkerService.RegisterServiceGrpc.stub(ConnectionManager.getWorkerChannel(workerIp))
          await { stub.introduceNewWorker(new WorkerNetworkInfo(request.ip, request.port)) }
        }
      }
    }
    RegisterWorkerResponse(success = true)
  }
}
