package state

import scala.concurrent.Promise
import scala.concurrent.Future
import global.WorkerState
import global.Restorable

class SynchronizationState extends Serializable with Restorable {
  private var shufflePlans: Map[String, Seq[String]] = Map.empty
  private var reportCompleted: Boolean = false
  private var shuffleStarted: Boolean = false
  @transient private val shuffleStartPromise: Promise[Unit] = Promise[Unit]()

  def restoreTransient(): Unit = {
    if (shuffleStarted) shuffleStartPromise.trySuccess()
  }
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

  def completeReport() = WorkerState.synchronized {
    WorkerState.synchronization.reportCompleted = true
  }

  def isReportCompleted = WorkerState.synchronized {
    WorkerState.synchronization.reportCompleted
  }

  def setShuffleStarted(value: Boolean) = WorkerState.synchronized {
    WorkerState.synchronization.shuffleStarted = value
  }

  def isShuffleStarted = WorkerState.synchronized {
    WorkerState.synchronization.shuffleStarted
  }
}