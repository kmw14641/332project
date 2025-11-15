package worker

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import worker.WorkerService.{WorkerServiceGrpc, WorkersRangeAssignment, WorkerRangeAssignment, WorkerNetworkInfo, RangeAssignment, AssignRangesResponse, StartShuffleCommand}
import com.google.protobuf.ByteString

class WorkerClient(host: String, port: Int)(implicit ec: ExecutionContext) {
  private val channel: ManagedChannel = ManagedChannelBuilder
    .forAddress(host, port)
    .usePlaintext()
    .build()

  private val stub = WorkerServiceGrpc.stub(channel)

  def assignRanges(assignments: Map[(String, Int), (ByteString, ByteString)]): Boolean = {
    val request = WorkersRangeAssignment(
      assignments = assignments.map { case ((ip, port), (start, end)) =>
        WorkerRangeAssignment(
          worker = Some(WorkerNetworkInfo(ip = ip, port = port)),
          range = Some(RangeAssignment(start = start, end = end))
        )
      }.toSeq
    )

    val responseFuture = stub.assignRanges(request)
    
    try {
      val response = Await.result(responseFuture, 10.seconds)
      response.success
    } catch {
      case e: Exception =>
        println(s"Error assigning range to worker: ${e.getMessage}")
        false
    }

    // TODO: global shutdown logic required
  }

  def shutdown(): Unit = {
    channel.shutdown()
  }

  def startShuffle(reason: String = "Shuffle phase start"): Boolean = {
    val request = StartShuffleCommand(reason = reason)
    val responseFuture = stub.startShuffle(request)

    try {
      val response = Await.result(responseFuture, 10.seconds)
      response.success
    } catch {
      case e: Exception =>
        println(s"Error starting shuffle phase on worker $host:$port: ${e.getMessage}")
        false
    }
  }
}
