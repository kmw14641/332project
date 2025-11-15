package worker

import scala.concurrent.{ExecutionContext, Future}
import worker.WorkerService.{WorkerServiceGrpc, WorkersRangeAssignment, RangeAssignment, WorkerNetworkInfo, AssignRangesResponse, WorkerRangeAssignment, FileListMessage, FileListAck, StartShuffleCommand, StartShuffleAck}
import io.grpc.{Status, StatusException}
import java.math.BigInteger
import worker.sync.SynchronizationManager

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
    val senderInfo = request.sender.getOrElse(
      throw new StatusException(Status.INVALID_ARGUMENT.withDescription("Sender info is missing"))
    )

    val files = request.files.map(meta => Worker.ReceivedFileInfo(meta.fileName))
    Worker.addIncomingFilePlan((senderInfo.ip, senderInfo.port), files)

    //for debugging
    val fileNames = files.map(_.fileName).mkString(", ")
    println(s"[Sync][RecvList] ${senderInfo.ip}:${senderInfo.port} -> files: [$fileNames]")
    println(s"Received ${files.size} file descriptions from ${senderInfo.ip}:${senderInfo.port}")

    Future.successful(FileListAck(success = true))
  }

}
