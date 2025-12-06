package global

import org.slf4j.LoggerFactory
import master.MasterService.WorkerInfo
import scala.collection.mutable.ArrayBuffer
import com.google.protobuf.ByteString
import common.data.Data.{Key, Record, getKeyOrdering}
import scala.concurrent.{Promise, Future}

// Master Singleton
object MasterState {
  private val logger = LoggerFactory.getLogger(getClass)
  
  private var workersNum: Int = -1
  private var registeredWorkers = Map[String, WorkerInfo]()
  private var samples = Map[String, Seq[Key]]()  // workerIp -> sampled keys
  private var calculateRangesStarted = false
  private var ranges = Map[(String, Int), Record]()  // (start, end) for each worker
  private var syncCompletedWorkers = Set[String]()
  private var shuffleStarted = false
  private var finalMergeCompletedWorkers = Set[String]()
  private var terminated = false
  private val shutdownPromise: Promise[Unit] = Promise[Unit]()

  def setWorkersNum(num: Int): Unit = this.synchronized {
    workersNum = num
  }

  def getWorkersNum: Int = this.synchronized {
    workersNum
  }

  // returns whether it was duplicated (i.e. fault occured)
  def registerWorker(request: WorkerInfo): Boolean = this.synchronized {
    val workerIp = request.ip
    if (registeredWorkers.contains(workerIp)) {
      logger.warn(s"Fault detected! Re-register worker($workerIp:${request.port})")
      logger.info(registeredWorkers.keys.mkString(", "))
      registeredWorkers += (workerIp -> request)
      true
    } else {
      registeredWorkers += (workerIp -> request)
      if (registeredWorkers.size == workersNum) {
        logger.info("all worker registered")
        logger.info(registeredWorkers.keys.mkString(", "))
      }
      false
    }
  }

  def getRegisteredWorkers: Map[String, WorkerInfo] = this.synchronized { registeredWorkers }

  def addSamples(workerIp: String, keys: Seq[Key]): Boolean = this.synchronized {
    if (!registeredWorkers.contains(workerIp)) {
      logger.warn(s"Warning: Received samples from unregistered worker: $workerIp")
      return false
    }

    samples += (workerIp -> keys)
    true
  }

  def getSampleSize: Int = this.synchronized { samples.size }

  def tryStartCalculateRanges(): Boolean = this.synchronized {
    if (calculateRangesStarted) false
    else {
      calculateRangesStarted = true
      true
    }
  }

  def calculateRanges(): Unit = this.synchronized {
    implicit val ordering = getKeyOrdering
    val sortedKeys = samples.values.flatten.toArray.sorted

    // Calculate quantiles to divide into workersNum ranges
    val workers = registeredWorkers.toSeq.sortBy(_._1).map {
      case (ip, info) => (ip, info.port)
    }
    // n / total * len  => index of worker_n's criterion of ranges using balancing distribution
    val rangesSeq = (1 until workersNum).map { i =>
      val idx = ((i.toDouble / workersNum) * sortedKeys.length).toInt
      sortedKeys(math.max(0, idx))
    }

    val rangeBuffer = ArrayBuffer[Record]()
    var previousKey = ByteString.copyFrom(Array.fill[Byte](10)(0))
    for (key <- rangesSeq) {
      rangeBuffer.append((previousKey, key))
      previousKey = key
    }
    rangeBuffer.append((previousKey, ByteString.copyFrom(Array.fill[Byte](1)(1) ++ Array.fill[Byte](10)(0))))  // Last range to infinity

    ranges = workers.zip(rangeBuffer).map {
      case ((ip, port), (start, end)) => ((ip, port) -> (start, end))
    }.toMap
  }

  def getRanges: Map[(String, Int), Record] = this.synchronized { ranges }

  def isRangesReady: Boolean = this.synchronized { ranges.nonEmpty }

  /*
  Start timing for shuffle phase is synchronized 
  since markSyncCompleted blocks to ensure all workers have reported completion
  before starting the shuffle phase.
  */
  def markSyncCompleted(workerIp: String): (Boolean, Int, Int) = this.synchronized {
    if (!registeredWorkers.contains(workerIp)) {
      logger.warn(s"Ignoring sync completion from unknown worker $workerIp")
      return (false, syncCompletedWorkers.size, registeredWorkers.size)
    }

    syncCompletedWorkers += workerIp
    (syncCompletedWorkers.size == registeredWorkers.size, syncCompletedWorkers.size, registeredWorkers.size)
  }

  def markShuffleStarted(): Unit = this.synchronized {
    shuffleStarted = true
  }

  def hasShuffleStarted: Boolean = this.synchronized { shuffleStarted }

  def markFinalMergeCompleted(workerIp: String): Unit = this.synchronized {
    assert(registeredWorkers.contains(workerIp))  // is this really helpful?
    finalMergeCompletedWorkers += workerIp
  }

  def allFinalMergeCompleted: Boolean = this.synchronized {
    finalMergeCompletedWorkers.size == registeredWorkers.size
  }

  def markTerminated(): Unit = this.synchronized { terminated = true }

  def isTerminated: Boolean = this.synchronized { terminated }

  def signalShutdown(): Unit = {
    if (!shutdownPromise.isCompleted) {
      shutdownPromise.success(())
    }
  }

  def awaitShutdown: Future[Unit] = shutdownPromise.future
}
