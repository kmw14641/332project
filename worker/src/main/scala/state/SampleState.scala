package state

import common.data.Data.Key
import scala.concurrent.Promise
import scala.concurrent.Future
import global.WorkerState
import global.Restorable

class SampleState extends Serializable with Restorable {
  private var sendSampleCompleted = false
  private var assignedRange: Option[Map[(String, Int), (Key, Key)]] = None
  @transient private val assignPromise: Promise[Unit] = Promise[Unit]()

  def restoreTransient(): Unit = {
    assignedRange.foreach(_ => assignPromise.trySuccess())
  }
}

object SampleState {
  def completeSendSample() = WorkerState.synchronized {
    WorkerState.sample.sendSampleCompleted = true
  }

  def isSendSampleCompleted = WorkerState.synchronized {
    WorkerState.sample.sendSampleCompleted
  }

  def setAssignedRange(assignments: Map[(String, Int), (Key, Key)]): Unit = WorkerState.synchronized {
    WorkerState.sample.assignedRange = Some(assignments)
  }

  def getAssignedRange: Option[Map[(String, Int), (Key, Key)]] = WorkerState.synchronized {
    WorkerState.sample.assignedRange
  }

  def markAssigned(): Unit = WorkerState.sample.assignPromise.trySuccess()
  def waitForAssignment(): Future[Unit] = WorkerState.sample.assignPromise.future
}