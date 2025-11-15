package master

import master.MasterService.WorkerInfo
import scala.collection.mutable.ArrayBuffer
import com.google.protobuf.ByteString

// Master Singleton
object Master {
  private var workersNum: Int = -1
  private var registeredWorkers = Map[String, WorkerInfo]()
  private var samples = Map[String, Seq[ByteString]]()  // workerIp -> sampled keys
  private var ranges = Map[(String, Int), (ByteString, ByteString)]()  // (start, end) for each worker
  private var syncCompletedWorkers = Set[String]()
  private var shuffleStarted = false

  def setWorkersNum(num: Int): Unit = this.synchronized {
    workersNum = num
  }

  def getWorkersNum: Int = this.synchronized {
    workersNum
  }

  def registerWorker(request: WorkerInfo): Boolean = this.synchronized {
    val workerIp = request.ip
    if (registeredWorkers.size == workersNum) {
      if (!registeredWorkers.contains(workerIp)) false
      else {
        registeredWorkers += (workerIp -> request)
        println(s"Fault detected! Re-register worker($workerIp:${request.port})")
        println(registeredWorkers.keys.mkString(", "))

        true
      }
    }
    else {
      val previouslyFull = registeredWorkers.size == workersNum
      registeredWorkers += (workerIp -> request)
      if (!previouslyFull && registeredWorkers.size == workersNum) {
        println(registeredWorkers.keys.mkString(", "))
      }

      true
    }
  }

  def getRegisteredWorkers: Map[String, WorkerInfo] = this.synchronized { registeredWorkers }

  def addSamples(workerIp: String, keys: Seq[ByteString]): Boolean = this.synchronized {
    if (!registeredWorkers.contains(workerIp)) {
      println(s"Warning: Received samples from unregistered worker: $workerIp")
      return false
    }

    samples += (workerIp -> keys)
    true
  }

  def getSampleSize: Int = this.synchronized { samples.size }

  def calculateRanges(): Unit = this.synchronized {
    val comparator = ByteString.unsignedLexicographicalComparator()
    val sortedKeys = samples.values.flatten.toArray.sortWith((a, b) => comparator.compare(a, b) < 0)

    // Calculate quantiles to divide into workersNum ranges
    val workers = registeredWorkers.toSeq.sortBy(_._1).map {
      case (ip, info) => (ip, info.port)
    }
    // n / total * len  => index of worker_n's criterion of ranges using balancing distribution
    val rangesSeq = (1 until workersNum).map { i =>
      val idx = ((i.toDouble / workersNum) * sortedKeys.length).toInt
      sortedKeys(math.max(0, idx))
    }

    val rangeBuffer = ArrayBuffer[(ByteString, ByteString)]()
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

  def getRanges: Map[(String, Int), (ByteString, ByteString)] = this.synchronized { ranges }

  def isRangesReady: Boolean = this.synchronized { ranges.nonEmpty }

  /*
  Start timing for shuffle phase is synchronized 
  since markSyncCompleted blocks to ensure all workers have reported completion
  before starting the shuffle phase.
  */
  def markSyncCompleted(workerIp: String): (Boolean, Int, Int) = this.synchronized {
    if (!registeredWorkers.contains(workerIp)) {
      println(s"Ignoring sync completion from unknown worker $workerIp")
      return (false, syncCompletedWorkers.size, registeredWorkers.size)
    }

    syncCompletedWorkers += workerIp
    (syncCompletedWorkers.size == registeredWorkers.size, syncCompletedWorkers.size, registeredWorkers.size)
  }

  def resetSyncState(): Unit = this.synchronized {
    syncCompletedWorkers = Set.empty
    shuffleStarted = false
  }

  def markShuffleStarted(): Unit = this.synchronized {
    shuffleStarted = true
  }

  def hasShuffleStarted: Boolean = this.synchronized { shuffleStarted }
}
