package main

import scala.concurrent.ExecutionContext
import master.MasterService.MasterServiceGrpc
import global.ConnectionManager
import scala.concurrent.Future
import common.utils.SystemUtils
import master.MasterService.WorkerInfo
import scala.concurrent.Await
import scala.concurrent.duration._

class RegisterManager(implicit ec: ExecutionContext) {
  private val stub = MasterServiceGrpc.stub(ConnectionManager.getMasterChannel())

  def start(port: Int): Unit = {
    val workerIp = SystemUtils.getLocalIp.getOrElse {
        println("Failed to get local IP address")
        sys.exit(1)
    }
    val ramMb = SystemUtils.getRamMb

    val request = WorkerInfo(
      ip = workerIp,
      port = port,
      ramMb = ramMb
    )

    val responseFuture = stub.registerWorker(request)
    
    try {
      val response = Await.result(responseFuture, 10.seconds)
      if (!response.success) {
        println(s"Failed to connect to master")
        sys.exit(1)
      }
    } catch {
      case e: Exception =>
        println(s"Error registering with master: ${e.getMessage}")
        sys.exit(1)
    }
  }
}