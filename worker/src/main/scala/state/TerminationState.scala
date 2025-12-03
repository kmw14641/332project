package state

import scala.concurrent.Promise
import scala.concurrent.Future
import global.WorkerState

class TerminationState extends Serializable {
  @transient private val terminatePromise: Promise[Unit] = Promise[Unit]()
}

object TerminationState {
  def waitForTerminate: Future[Unit] = WorkerState.terminate.terminatePromise.future
  def markTerminated(): Unit = WorkerState.terminate.terminatePromise.trySuccess()
}