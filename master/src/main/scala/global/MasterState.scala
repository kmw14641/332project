package global

import org.slf4j.LoggerFactory
import master.MasterService.WorkerInfo
import scala.collection.mutable.ArrayBuffer
import common.data.Data.getKeyOrdering
import scala.concurrent.{Promise, Future}

// Master Singleton
object MasterState {
  private val logger = LoggerFactory.getLogger(getClass)
  
  private var workersNum: Int = -1
  private var registeredWorkers = Map[String, WorkerInfo]()
  private var samples = Map[String, Seq[Array[Byte]]]()  // workerIp -> sampled keys
  private var calculateRangesStarted = false
  private var ranges = Map[(String, Int), (Array[Byte], Array[Byte])]()  // (start, end) for each worker
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
  // (isRegistered, faultOccured)
  def registerWorker(request: WorkerInfo): (Boolean, Boolean) = this.synchronized {
    val workerIp = request.ip
    val isAllWorkersRegistered = registeredWorkers.size == workersNum
    val isContained = registeredWorkers.contains(workerIp)
    if (isAllWorkersRegistered && !isContained) {
      logger.warn(s"Warning: Extra worker registration attempt from $workerIp after all workers have been registered.")
      return (false, false)
    }

    registeredWorkers += (workerIp -> request)
    if (isContained) {
      logger.warn(s"Fault detected! Re-register worker($workerIp:${request.port})")
      logger.info(registeredWorkers.keys.mkString(", "))
    } else {
      if (registeredWorkers.size == workersNum) {
        logger.info("all worker registered")
        println(registeredWorkers.keys.mkString(", "))
      }
    }

    (true, isContained)
  }

  def getRegisteredWorkers: Map[String, WorkerInfo] = this.synchronized { registeredWorkers }

  def addSamples(workerIp: String, keys: Seq[Array[Byte]]): Boolean = this.synchronized {
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

    val rangeBuffer = ArrayBuffer[(Array[Byte], Array[Byte])]()
    var previousKey = new Array[Byte](10) // 0-filled by default
    for (key <- rangesSeq) {
      rangeBuffer.append((previousKey, key))
      previousKey = key
    }
    
    val lastKey = new Array[Byte](11)
    lastKey(0) = 1 // The rest are 0s
    rangeBuffer.append((previousKey, lastKey))  // Last range to infinity

    ranges = workers.zip(rangeBuffer).map {
      case ((ip, port), (start, end)) => ((ip, port) -> (start, end))
    }.toMap
  }

  def getRanges: Map[(String, Int), (Array[Byte], Array[Byte])] = this.synchronized { ranges }

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
