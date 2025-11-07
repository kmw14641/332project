import io.grpc.ServerBuilder
import scala.concurrent.ExecutionContext
import greeter.Greeter.GreeterGrpc
import greeter.GreeterImpl
import greeter.GreeterClient
import utils.MasterOptionUtils

object Main extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global
  
  val workersNum = MasterOptionUtils.parse(args) match {
    case Some(num) => num
    case None => sys.exit(1)
  }

  val ip = utils.AddrUtils.getAddr match {
    case Some(addr) => addr
    case None =>
      println("Failed to get local IP address")
      sys.exit(1)
  }

  val server = ServerBuilder
  .forPort(0)
  .addService(GreeterGrpc.bindService(new GreeterImpl(), ec))
  .build()

  server.start()

  val port = server.getPort
  println(s"$ip:$port")

  new GreeterClient().fansign()

  server.awaitTermination()
}