package greeter

import scala.concurrent.{Future, ExecutionContext}
import greeter.Greeter.{GreeterGrpc, HelloReply, HelloRequest}
import io.grpc.ManagedChannelBuilder

class GreeterClient(implicit ec: ExecutionContext) {
  def sayHello() = {
    val channel = ManagedChannelBuilder.forAddress("127.0.0.1", 8080).usePlaintext().build
    val request = HelloRequest(name = "World")
    val stub = GreeterGrpc.stub(channel)
    for {
      response <- stub.sayHello(request)
    } yield {
      println(response.message)
      channel.shutdown()
    }
  }
}