package state

import scala.concurrent.Promise
import scala.concurrent.Future
import global.WorkerState
import global.Restorable
import global.StateRestoreManager
import common.utils.SystemUtils

class TerminationState extends Serializable with Restorable {
  private var isRestored = false
  @transient private lazy val terminatePromise: Promise[Unit] = Promise[Unit]()

  def restoreTransient(): Unit = {}
}

object TerminationState {
  def waitForTerminate: Future[Unit] = WorkerState.terminate.terminatePromise.future
  def markTerminated(): Unit = WorkerState.terminate.terminatePromise.trySuccess()
  def shutdownOnce() = {
    if (SystemUtils.getLocalIp.get == "2.2.2.103" == !WorkerState.terminate.isRestored) {
      WorkerState.terminate.isRestored = true
      StateRestoreManager.storeState()
      println("shutdown once")
      System.exit(0)
    }
  }
}