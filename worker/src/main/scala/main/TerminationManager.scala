package main

import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import global.ConnectionManager
import master.MasterService.{FinalMergeServiceGrpc, FinalMergePhaseReport}
import scala.async.Async.{async, await}
import common.utils.SystemUtils
import io.grpc.Server
import state.TerminationState

class TerminationManager(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)

  private val masterStub = FinalMergeServiceGrpc.stub(ConnectionManager.getMasterChannel())

  def shutdownServerSafely(server: Server): Future[Unit] = async {
    val request = FinalMergePhaseReport(workerIp = SystemUtils.getLocalIp)
    await { masterStub.reportFinalMergeCompletion(request) }
    await { TerminationState.waitForTerminate }

    ConnectionManager.shutdownAllChannels()
    server.shutdown()
    server.awaitTermination()
    ()
  }
}