package worker

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import worker.WorkerService.{WorkerServiceGrpc, RangeAssignment, RangeResponse}
import com.google.protobuf.ByteString

class WorkerClient(host: String, port: Int)(implicit ec: ExecutionContext) {
  private val channel: ManagedChannel = ManagedChannelBuilder
    .forAddress(host, port)
    .usePlaintext()
    .build()

  private val stub = WorkerServiceGrpc.stub(channel)

  def sampled(rangeStart: ByteString, rangeEnd: ByteString): Boolean = {
    val request = RangeAssignment(
      rangeStart = rangeStart,
      rangeEnd = rangeEnd
    )

    val responseFuture = stub.sampled(request)
    
    try {
      val response = Await.result(responseFuture, 10.seconds)
      response.success
    } catch {
      case e: Exception =>
        println(s"Error assigning range to worker: ${e.getMessage}")
        false
    } finally {
      shutdown()
    }
  }

  def shutdown(): Unit = {
    channel.shutdown()
  }
}
