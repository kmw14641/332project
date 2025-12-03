package state

import scala.concurrent.Promise
import scala.concurrent.Future
import global.WorkerState
import global.Restorable

class TerminationState extends Serializable with Restorable {
  @transient private val terminatePromise: Promise[Unit] = Promise[Unit]()

  def restoreTransient(): Unit = {}
}

object TerminationState {
  def waitForTerminate: Future[Unit] = WorkerState.terminate.terminatePromise.future
  def markTerminated(): Unit = WorkerState.terminate.terminatePromise.trySuccess()
}