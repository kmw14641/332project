package worker

import scala.concurrent.{ExecutionContext, Future}
import worker.WorkerService.{WorkerServiceGrpc, WorkersRangeAssignment, RangeAssignment, WorkerNetworkInfo, AssignRangesResponse, WorkerRangeAssignment, FileListMessage, FileListAck, StartShuffleCommand, StartShuffleAck}
import io.grpc.{Status, StatusException}
import java.math.BigInteger
import worker.sync.SynchronizationManager
import common.utils.SystemUtils

class WorkerServiceImpl(implicit ec: ExecutionContext) extends WorkerServiceGrpc.WorkerService {
  override def assignRanges(request: WorkersRangeAssignment): Future[AssignRangesResponse] = {
    val workersRangeAssignment = request.assignments.map {
      case workerRange => {
        val workerInfo: WorkerNetworkInfo = workerRange.worker.getOrElse(
          throw new StatusException(Status.INVALID_ARGUMENT.withDescription("WorkerNetworkInfo is missing"))
        )
        val rangeInfo: RangeAssignment = workerRange.range.getOrElse(
          throw new StatusException(Status.INVALID_ARGUMENT.withDescription("RangeAssignment is missing"))
        )

        (workerInfo.ip, workerInfo.port) -> (rangeInfo.start, rangeInfo.end)
      }
    }.toMap

    // Store the assigned range in the Worker singleton
    Worker.setAssignedRange(workersRangeAssignment)
    workersRangeAssignment.foreach {
      // Print assigned ranges for debugging
      case ((ip, port), (start, end)) =>
        val startInt = new BigInteger(1, start.toByteArray())
        val endInt = new BigInteger(1, end.toByteArray())
        println(s"Assigned range to worker $ip:$port => [${startInt.toString(16)}, ${endInt.toString(16)})")
    }

    val response = Future.successful(
      AssignRangesResponse(success = true)
    )

    SynchronizationManager.triggerSyncPhase()

    response
  }

  /*
  Receive file metadata list from a peer worker.
  Store the incoming file plans in the Worker singleton for later processing.
  */
  override def deliverFileList(request: FileListMessage): Future[FileListAck] = {
    val senderIp = request.senderIp    
    val files = request.files.map(_.fileName)
    Worker.addIncomingFilePlan(senderIp, files)

    //for debugging
    val fileNames = files.mkString(", ")
    println(s"[Sync][RecvList] $senderIp -> files: [$fileNames]")
    println(s"Received ${files.size} file descriptions from $senderIp")

    Future.successful(FileListAck(success = true))
  }

  override def startShuffle(request: StartShuffleCommand): Future[StartShuffleAck] = {
    if (!Worker.hasReceivedShuffleCommand) {
      println(s"Received shuffle start command. Reason: ${request.reason}")
    }
    /*
    By marking shuffleStartPromise to success, 
    unblock any waiting synchronization manager.
    */ 
    Worker.markShuffleStarted()

    Future.successful(StartShuffleAck(success = true))
  }
}
