import io.grpc.ServerBuilder
import scala.concurrent.ExecutionContext
import greeter.Greeter.GreeterGrpc
import greeter.GreeterImpl
import greeter.GreeterClient
import utils.MasterOptionUtils
import server.MasterServiceImpl
import global.MasterState
import master.MasterService.MasterServiceGrpc
import common.utils.SystemUtils

object Main extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global

  val workersNum = MasterOptionUtils.parse(args).getOrElse {
    sys.exit(1)
  }

  MasterState.setWorkersNum(workersNum)

  val ip = SystemUtils.getLocalIp.getOrElse {
    println("Failed to get local IP address")
    sys.exit(1)
  }

  val server = ServerBuilder
    .forPort(0)
    .addService(MasterServiceGrpc.bindService(new MasterServiceImpl(), ec))
    .build()

  server.start()

  val port = server.getPort
  println(s"$ip:$port")

  // new GreeterClient().fansign()

  server.awaitTermination()
}