package worker

import com.google.protobuf.ByteString
import scala.concurrent.{Future, Promise}

// Worker Singleton
object Worker {
  private var masterIp: Option[String] = None
  private var masterPort: Option[Int] = None
  private var inputDirs: Seq[String] = Nil
  private var outputDir: Option[String] = None
  private var assignedRange: Option[Map[(String, Int), (ByteString, ByteString)]] = None
  private var assignedFiles: Map[(String, Int), List[String]] = Map.empty
  private var workerIp: Option[String] = None
  private var workerPort: Option[Int] = None
  private var incomingFilePlans = Map[String, Seq[String]]()
  private val shuffleStartPromise: Promise[Unit] = Promise[Unit]()

  def setMasterAddr(ip: String, port: Int): Unit = this.synchronized {
    masterIp = Some(ip)
    masterPort = Some(port)
  }

  def getMasterAddr: Option[(String, Int)] = this.synchronized {
    for {
      ip <- masterIp
      port <- masterPort
    } yield (ip, port)
  }

  def setInputDirs(dirs: Seq[String]): Unit = this.synchronized {
    inputDirs = dirs
  }

  def getInputDirs: Seq[String] = this.synchronized {
    inputDirs
  }

  def setOutputDir(dir: String): Unit = this.synchronized {
    outputDir = Some(dir)
  }

  def getOutputDir: Option[String] = this.synchronized {
    outputDir
  }

  def setAssignedRange(assignments: Map[(String, Int), (ByteString, ByteString)]): Unit = this.synchronized {
    assignedRange = Some(assignments)
  }

  def getAssignedRange: Option[Map[(String, Int), (ByteString, ByteString)]] = this.synchronized {
    assignedRange
  }

  def setAssignedFiles(files: Map[(String, Int), List[String]]): Unit = this.synchronized {
    assignedFiles = files
  }

  def getAssignedFiles: Map[(String, Int), List[String]] = this.synchronized {
    assignedFiles
  }

  def setWorkerNetworkInfo(ip: String, port: Int): Unit = this.synchronized {
    workerIp = Some(ip)
    workerPort = Some(port)
  }

  def getWorkerNetworkInfo: Option[(String, Int)] = this.synchronized {
    for {
      ip <- workerIp
      port <- workerPort
    } yield (ip, port)
  }

  def addIncomingFilePlan(senderIp: String, files: Seq[String]): Unit = this.synchronized {
    val existing = incomingFilePlans.getOrElse(senderIp, Seq.empty)
    incomingFilePlans += senderIp -> (existing ++ files)
  }

  def getIncomingFilePlans: Map[String, Seq[String]] = this.synchronized {
    incomingFilePlans
  }

  def markShuffleStarted(): Unit = this.synchronized {
    if (!shuffleStartPromise.isCompleted) {
      shuffleStartPromise.success(())
    }
  }

  def waitForShuffleCommand: Future[Unit] = shuffleStartPromise.future

  def hasReceivedShuffleCommand: Boolean = shuffleStartPromise.isCompleted
}
