package main

import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext
import master.MasterService.{RegisterServiceGrpc, WorkerInfo}
import global.ConnectionManager
import scala.concurrent.Future
import common.utils.SystemUtils
import master.MasterService.WorkerInfo
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.async.Async.{async, await}

class RegisterManager(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)

  private val stub = RegisterServiceGrpc.stub(ConnectionManager.getMasterChannel())

  def start(port: Int): Future[Unit] = async {
    val workerIp = SystemUtils.getLocalIp.getOrElse {
        logger.error("Failed to get local IP address")
        sys.exit(1)
    }
    val ramMb = SystemUtils.getRamMb

    val request = WorkerInfo(
      ip = workerIp,
      port = port,
      ramMb = ramMb
    )

    await { stub.registerWorker(request) }

    ()
  }
}