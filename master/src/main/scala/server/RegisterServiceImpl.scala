package server

import scala.concurrent.{ExecutionContext, Future}
import master.MasterService.{WorkerInfo, RegisterWorkerResponse, RegisterServiceGrpc}
import global.{MasterState, ConnectionManager}
import scala.async.Async.{async, await}
import worker.WorkerService.WorkerServiceGrpc
import worker.WorkerService.WorkerNetworkInfo

class RegisterServiceImpl(implicit ec: ExecutionContext) extends RegisterServiceGrpc.RegisterService {
  override def registerWorker(request: WorkerInfo): Future[RegisterWorkerResponse] = Future {
    val faultOccured = MasterState.registerWorker(request)
    ConnectionManager.registerWorkerChannel(request.ip, request.port)
    if (faultOccured) {
      MasterState.getRegisteredWorkers.filter(_._1 != request.ip).map { case (workerIp, _) => async {
          val stub = WorkerServiceGrpc.stub(ConnectionManager.getWorkerChannel(workerIp))
          await { stub.introduceNewWorker(new WorkerNetworkInfo(request.ip, request.port)) }
        }
      }
    }
    RegisterWorkerResponse(success = true)
  }
}
