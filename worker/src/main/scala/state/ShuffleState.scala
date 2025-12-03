package state

import global.Restorable
import scala.collection.mutable
import global.WorkerState

class ShuffleState extends Serializable with Restorable {
  private var shufflePlans: Option[Map[String, mutable.Map[String, Boolean]]] = None
  private var shuffleCompleted: Boolean = false

  def restoreTransient(): Unit = {}
}

object ShuffleState {
  def setShufflePlans(shufflePlans: Map[String, mutable.Map[String, Boolean]]) = WorkerState.synchronized {
    WorkerState.shuffle.shufflePlans = Some(shufflePlans)
  }

  def getShufflePlans = WorkerState.synchronized {
    WorkerState.shuffle.shufflePlans
  }

  def completeShufflePlanFile(workerIp: String, fileName: String) = WorkerState.synchronized {
    WorkerState.shuffle.shufflePlans.get(workerIp)(fileName) = true
  }

  def completeShuffle() = WorkerState.synchronized {
    WorkerState.shuffle.shuffleCompleted = true
  }

  def isShuffleCompleted = WorkerState.synchronized {
    WorkerState.shuffle.shuffleCompleted
  }
}