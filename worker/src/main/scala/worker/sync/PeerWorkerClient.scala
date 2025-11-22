package worker.sync

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import worker.WorkerService.{WorkerServiceGrpc, FileMetadata, FileListMessage}

class PeerWorkerClient(host: String, port: Int)(implicit ec: ExecutionContext) {
  private val channel: ManagedChannel = ManagedChannelBuilder
    .forAddress(host, port)
    .usePlaintext()
    .build()

  private val stub = WorkerServiceGrpc.stub(channel)

  def deliverFileList(senderIp: String, files: Seq[FileMetadata]): Future[Boolean] = {
    val request = FileListMessage(
      senderIp = senderIp,
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
