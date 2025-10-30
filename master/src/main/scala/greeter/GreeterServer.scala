package greeter

import scala.concurrent.{Future, ExecutionContext}
import greeter.Greeter.{GreeterGrpc, HelloReply, HelloRequest}

class GreeterImpl(implicit ec: ExecutionContext) extends GreeterGrpc.Greeter {
  override def sayHello(req: HelloRequest) = {
    val reply = HelloReply(message = "Hello " + req.name)
    Future.successful(reply)
  }
}