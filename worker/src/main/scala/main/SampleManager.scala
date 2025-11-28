package main

import scala.concurrent.ExecutionContext
import master.MasterService.MasterServiceGrpc
import global.ConnectionManager
import scala.concurrent.Future
import utils.SamplingUtils
import master.MasterService.SampleData
import scala.concurrent.Await
import scala.concurrent.duration._
import common.utils.SystemUtils
import global.WorkerState

class SampleManager(implicit ec: ExecutionContext) {
  private val stub = MasterServiceGrpc.stub(ConnectionManager.getMasterChannel())

  def start(): Future[Unit] = Future {
    val workerIp = SystemUtils.getLocalIp.getOrElse {
      println("Failed to get local IP address")
      sys.exit(1)
    }

    try {
      val samples = SamplingUtils.sampleFromInputs(WorkerState.getInputDirs).getOrElse {
        println("Warning: Sampling failed")
        sys.exit(1)
      }
    
      val success = {
        val request = SampleData(
          workerIp = workerIp,
          keys = samples
        )

        val responseFuture = stub.sampling(request)
        
        try {
          val response = Await.result(responseFuture, 30.seconds)
          response.success
        } catch {
          case e: Exception =>
            println(s"Error sending samples to master: ${e.getMessage}")
            e.printStackTrace()
            false
        }
      }
    
      if (success) {
        println("Samples sent successfully. Waiting for range assignment...")
      } else {
        println("Failed to send samples to master")
      }
    } catch {
    case e: Exception =>
        println(s"Error during sampling: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}