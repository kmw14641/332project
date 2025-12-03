package state

import common.data.Data.Key
import scala.concurrent.Promise
import scala.concurrent.Future
import global.WorkerState

class SampleState extends Serializable {
  private var assignedRange: Option[Map[(String, Int), (Key, Key)]] = None
  @transient private val assignPromise: Promise[Unit] = Promise[Unit]()
}

object SampleState {
  def setAssignedRange(assignments: Map[(String, Int), (Key, Key)]): Unit = WorkerState.synchronized {
    WorkerState.sample.assignedRange = Some(assignments)
  }

  def getAssignedRange: Option[Map[(String, Int), (Key, Key)]] = WorkerState.synchronized {
    WorkerState.sample.assignedRange
  }

  def markAssigned(): Unit = WorkerState.sample.assignPromise.trySuccess()
  def waitForAssignment(): Future[Unit] = WorkerState.sample.assignPromise.future
}