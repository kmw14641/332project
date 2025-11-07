import io.grpc.ServerBuilder
import scala.concurrent.ExecutionContext
import greeter.Greeter.GreeterGrpc
import greeter.GreeterImpl
import greeter.GreeterClient
import utils.MasterOptionUtils

object Main extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global

  val workersNum = MasterOptionUtils.parse(args).getOrElse {
    sys.exit(1)
  }

  println(s"Starting master with $workersNum workers")
}