package server

import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext
import worker.WorkerService.SyncServiceGrpc
import scala.concurrent.Future
import worker.WorkerService.FileListAck
import worker.WorkerService.FileListMessage
import worker.WorkerService.StartShuffleAck
import worker.WorkerService.StartShuffleCommand
import state.SynchronizationState
import global.StateRestoreManager

class SyncServiceImpl(implicit ec: ExecutionContext) extends SyncServiceGrpc.SyncService {
  private val logger = LoggerFactory.getLogger(getClass)

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
    logger.info(s"[Sync][RecvList] $senderIp -> files: [$fileNames]")
    logger.info(s"Received ${files.size} file descriptions from $senderIp")
    FileListAck(success = true)
  }

  override def startShuffle(request: StartShuffleCommand): Future[StartShuffleAck] = {
    if (!SynchronizationState.hasReceivedShuffleCommand) {
      logger.info(s"Received shuffle start command. Reason: ${request.reason}")
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
}