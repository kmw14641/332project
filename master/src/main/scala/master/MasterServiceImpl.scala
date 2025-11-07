package master

import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future}
import master.MasterService.{MasterServiceGrpc, WorkerInfo, RegisterWorkerResponse}
import master.Master

// TODO: handling fault tolerance
// 1. worker shutdown after Master.registerWorker
// 2. worker shutdown after return the registerWorker
// => then in sampling phase, 1 or 2 must be delayed so
// 1. worker gives registerWorker again => just update
// 2. worker gives samples => register worker works well, dont care about it
class MasterServiceImpl(implicit ec: ExecutionContext) extends MasterServiceGrpc.MasterService {
  override def registerWorker(request: WorkerInfo): Future[RegisterWorkerResponse] = {
    Master.registerWorker(request)

    Future.successful(
      RegisterWorkerResponse(success = true)
    )
  }
}
