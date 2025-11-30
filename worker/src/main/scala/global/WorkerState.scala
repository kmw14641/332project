package global

import scala.concurrent.{Future, Promise}
import common.data.Data.Key
import com.google.protobuf.ByteString

class WorkerState extends Serializable {
  var masterIp: Option[String] = None
  var masterPort: Option[Int] = None
  var assignedRange: Option[Map[(String, Int), (Key, Key)]] = None
  var assignedFiles: Map[(String, Int), List[String]] = Map.empty
  var shufflePlans: Map[String, Seq[String]] = Map.empty
  @transient val assignPromise: Promise[Unit] = Promise[Unit]()
  @transient val shuffleStartPromise: Promise[Unit] = Promise[Unit]()
  @transient val terminatePromise: Promise[Unit] = Promise[Unit]()
}

object WorkerState {
  var instance: WorkerState = new WorkerState()

  def setInstance(newInstance: WorkerState): Unit = this.synchronized {
    instance = newInstance
  }

  def getInstance: WorkerState = this.synchronized {
    instance
  }

  def setMasterAddr(ip: String, port: Int): Unit = this.synchronized {
    instance.masterIp = Some(ip)
    instance.masterPort = Some(port)
  }

  def getMasterAddr: Option[(String, Int)] = this.synchronized {
    for {
      ip <- instance.masterIp
      port <- instance.masterPort
    } yield (ip, port)
  }

  def setAssignedRange(assignments: Map[(String, Int), (Key, Key)]): Unit = this.synchronized {
    instance.assignedRange = Some(assignments)
    instance.assignPromise.trySuccess(())
  }

  def getAssignedRange: Option[Map[(String, Int), (Key, Key)]] = this.synchronized {
    instance.assignedRange
  }

  def waitForAssignment(): Future[Unit] = this.synchronized {
    instance.assignPromise.future
  }

  def setAssignedFiles(files: Map[(String, Int), List[String]]): Unit = this.synchronized {
    instance.assignedFiles = files
  }

  def getAssignedFiles: Map[(String, Int), List[String]] = this.synchronized {
    instance.assignedFiles
  }

  def addShufflePlan(senderIp: String, files: Seq[String]): Unit = this.synchronized {
    val existing = instance.shufflePlans.getOrElse(senderIp, Seq.empty)
    instance.shufflePlans += senderIp -> (existing ++ files)
  }

  def getShufflePlans: Map[String, Seq[String]] = this.synchronized {
    instance.shufflePlans
  }

  def markShuffleStarted(): Unit = this.synchronized {
    instance.shuffleStartPromise.trySuccess(())
  }

  def waitForShuffleCommand: Future[Unit] = this.synchronized {
    instance.shuffleStartPromise.future
  }

  def hasReceivedShuffleCommand: Boolean = this.synchronized {
    instance.shuffleStartPromise.isCompleted
  }

  def waitForTerminate: Future[Unit] = this.synchronized {
    instance.terminatePromise.future
  }

  def markTerminated(): Unit = this.synchronized {
    instance.terminatePromise.trySuccess(())
  }
}
