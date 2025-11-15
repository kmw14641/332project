package worker.sync

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import worker.WorkerService.{WorkerServiceGrpc, WorkerNetworkInfo, FileMetadata, FileListMessage}

class PeerWorkerClient(host: String, port: Int)(implicit ec: ExecutionContext) {
  private val channel: ManagedChannel = ManagedChannelBuilder
    .forAddress(host, port)
    .usePlaintext()
    .build()

  private val stub = WorkerServiceGrpc.stub(channel)

  def deliverFileList(sender: WorkerNetworkInfo, files: Seq[FileMetadata]): Future[Boolean] = {
    val request = FileListMessage(
      sender = Some(sender),
      files = files
    )

    stub.deliverFileList(request).map(_.success).recover {
      case e: Exception =>
        println(s"Error delivering file list to $host:$port - ${e.getMessage}")
        false
    }
  }

  def shutdown(): Unit = {
    channel.shutdown()
  }
}
