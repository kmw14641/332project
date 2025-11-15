package master

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import master.MasterService.{MasterServiceGrpc, WorkerInfo, RegisterWorkerResponse, SampleData, SampleResponse, SyncPhaseReport, SyncPhaseAck}
import com.google.protobuf.ByteString

class MasterClient(host: String, port: Int)(implicit ec: ExecutionContext) {
  private val channel: ManagedChannel = ManagedChannelBuilder
    .forAddress(host, port)
    .usePlaintext()
    .build()

  private val stub = MasterServiceGrpc.stub(channel)

  def registerWorker(ip: String, port: Int, ramMb: Long): Unit = {
    val request = WorkerInfo(
      ip = ip,
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

  def sampling(workerIp: String, keys: Array[ByteString]): Boolean = {
    val request = SampleData(
      workerIp = workerIp,
      keys = keys
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

  def shutdown(): Unit = {
    channel.shutdown()
  }

  def reportSyncCompletion(workerIp: String): Future[Boolean] = {
    val request = SyncPhaseReport(workerIp = workerIp)
    stub.reportSyncCompletion(request).map(_.success).recover {
      case e: Exception =>
        println(s"Error reporting synchronization completion: ${e.getMessage}")
        false
    }
  }
}
