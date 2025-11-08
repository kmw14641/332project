import io.grpc.ServerBuilder
import scala.concurrent.ExecutionContext
import greeter.Greeter.GreeterGrpc
import greeter.GreeterImpl
import greeter.GreeterClient
import utils.{MasterOptionUtils, SystemUtils}
import master.{MasterServiceImpl, Master}
import master.MasterService.MasterServiceGrpc

object Main extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global

  val workersNum = MasterOptionUtils.parse(args).getOrElse {
    sys.exit(1)
  }

  Master.setWorkersNum(workersNum)

  val ip = SystemUtils.getAddr.getOrElse {
    println("Failed to get local IP address")
    sys.exit(1)
  }

  val masterService = new MasterServiceImpl()

  val server = ServerBuilder
    .forPort(0)
    .addService(MasterServiceGrpc.bindService(masterService, ec))
    .build()

  server.start()

  val port = server.getPort
  println(s"$ip:$port")

  // new GreeterClient().fansign()

  server.awaitTermination()
}