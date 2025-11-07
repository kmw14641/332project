import io.grpc.ServerBuilder
import scala.concurrent.ExecutionContext
import greeter.Greeter.GreeterGrpc
import greeter.GreeterImpl
import greeter.GreeterClient

object Main extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global

  val server = ServerBuilder
    .forPort(8080)
    .addService(GreeterGrpc.bindService(new GreeterImpl(), ec))
    .build()

  server.start()
  println("Server started on port 8080")

  new GreeterClient().fansign()

  server.awaitTermination()
}