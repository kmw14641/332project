import org.slf4j.LoggerFactory
import io.grpc.ServerBuilder
import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._
import utils.MasterOptionUtils
import server.{RegisterServiceImpl, SamplingServiceImpl, SyncAndShuffleServiceImpl, FinalMergeServiceImpl}
import global.{MasterState, ConnectionManager}
import master.MasterService.{RegisterServiceGrpc, SamplingServiceGrpc, SyncAndShuffleServiceGrpc, FinalMergeServiceGrpc}
import common.utils.SystemUtils

object Main extends App {
  private val logger = LoggerFactory.getLogger(getClass)
  
  implicit val ec: ExecutionContext = ExecutionContext.global

  val workersNum = MasterOptionUtils.parse(args).getOrElse {
    sys.exit(1)
  }

  MasterState.setWorkersNum(workersNum)

  val ip = SystemUtils.getLocalIp.getOrElse {
    logger.error("Failed to get local IP address")
    sys.exit(1)
  }

  val server = ServerBuilder
    .forPort(0)
    .addService(RegisterServiceGrpc.bindService(new RegisterServiceImpl(), ec))
    .addService(SamplingServiceGrpc.bindService(new SamplingServiceImpl(), ec))
    .addService(SyncAndShuffleServiceGrpc.bindService(new SyncAndShuffleServiceImpl(), ec))
    .addService(FinalMergeServiceGrpc.bindService(new FinalMergeServiceImpl(), ec))
    .build()

  server.start()

  val port = server.getPort
  println(s"$ip:$port")

  Await.result(MasterState.awaitShutdown, Duration.Inf)
  
  logger.info("Shutdown signal received. Initiating graceful shutdown...")
  server.shutdown()
  server.awaitTermination()
  
  // Cleanup after server termination
  ConnectionManager.shutdownAllChannels()
  logger.info("Master shutdown complete.")
}