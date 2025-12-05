package server

import scala.concurrent.{ExecutionContext, Future}
import worker.WorkerService.{WorkerServiceGrpc, WorkersRangeAssignment, RangeAssignment, WorkerNetworkInfo, AssignRangesResponse, WorkerRangeAssignment, FileListMessage, FileListAck, StartShuffleCommand, StartShuffleAck, TerminateCommand, TerminateAck, IntroduceAck}
import io.grpc.{Status, StatusException}
import java.math.BigInteger
import global.WorkerState
import global.ConnectionManager
import state.SampleState
import state.SynchronizationState
import state.TerminationState
import global.StateRestoreManager

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
    SampleState.setAssignedRange(workersRangeAssignment)
    StateRestoreManager.storeState()
    SampleState.markAssigned()

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

    response
  }

  /*
  Receive file metadata list from a peer worker.
  Store the incoming file plans in the Worker singleton for later processing.
  */
  override def deliverFileList(request: FileListMessage): Future[FileListAck] = Future {
    val senderIp = request.senderIp    
    val files = request.files
    SynchronizationState.setShufflePlan(senderIp, files)
    StateRestoreManager.storeState()

    // for debugging
    val fileNames = files.mkString(", ")
    println(s"[Sync][RecvList] $senderIp -> files: [$fileNames]")
    println(s"Received ${files.size} file descriptions from $senderIp")
    
    FileListAck(success = true)
  }

  override def startShuffle(request: StartShuffleCommand): Future[StartShuffleAck] = {
    if (!SynchronizationState.hasReceivedShuffleCommand) {
      println(s"Received shuffle start command. Reason: ${request.reason}")
    }
    /*
    By marking shuffleStartPromise to success, 
    unblock any waiting synchronization manager.
    */ 
    SynchronizationState.setShuffleStarted(true)
    StateRestoreManager.storeState()
    SynchronizationState.markShuffleStarted()

    Future.successful(StartShuffleAck(success = true))
  }

  override def terminate(request: TerminateCommand): Future[TerminateAck] = Future {
    TerminationState.markTerminated()
    TerminateAck(success = true)
  }

  override def introduceNewWorker(request: WorkerNetworkInfo): Future[IntroduceAck] = Future {
    ConnectionManager.setWorkerChannel(request.ip, request.port)
    IntroduceAck(success = true)
  }
}
