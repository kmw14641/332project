package state

import scala.concurrent.Promise
import scala.concurrent.Future
import global.WorkerState

class SynchronizationState extends Serializable {
  private var shufflePlans: Map[String, Seq[String]] = Map.empty
  @transient private val shuffleStartPromise: Promise[Unit] = Promise[Unit]()
}

object SynchronizationState {
  def markShuffleStarted(): Unit = WorkerState.synchronization.shuffleStartPromise.trySuccess()
  def waitForShuffleCommand: Future[Unit] = WorkerState.synchronization.shuffleStartPromise.future
  def hasReceivedShuffleCommand: Boolean = WorkerState.synchronization.shuffleStartPromise.isCompleted

  def addShufflePlan(senderIp: String, files: Seq[String]): Unit = WorkerState.synchronized {
    val existing = WorkerState.synchronization.shufflePlans.getOrElse(senderIp, Seq.empty)
    WorkerState.synchronization.shufflePlans += senderIp -> (existing ++ files)
  }

  def getShufflePlans: Map[String, Seq[String]] = WorkerState.synchronized {
    WorkerState.synchronization.shufflePlans
  }
}