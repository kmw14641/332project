package worker.sync

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import worker.WorkerService.{WorkerServiceGrpc, WorkerNetworkInfo, FileMetadata, FileListMessage}

class PeerWorkerClient(host: String, port: Int)(implicit ec: ExecutionContext) {
  private val channel: ManagedChannel = ManagedChannelBuilder
    .forAddress(host, port)
    .usePlaintext()
    .build()

  private val stub = WorkerServiceGrpc.stub(channel)

  def deliverFileList(sender: WorkerNetworkInfo, files: Seq[FileMetadata]): Boolean = {
    val request = FileListMessage(
      sender = Some(sender),
      files = files
    )

    val responseFuture = stub.deliverFileList(request)

    try {
      val response = Await.result(responseFuture, 15.seconds)
      response.success
    } catch {
      case e: Exception =>
        println(s"Error delivering file list to $host:$port - ${e.getMessage}")
        false
    }
  }

  def shutdown(): Unit = {
    channel.shutdown()
  }
}
