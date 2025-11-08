package master

import master.MasterService.WorkerInfo

// Master Singleton
object Master {
  private var workersNum: Int = -1
  private var registeredWorkers = Map[String, WorkerInfo]()
  private var samples = Map[String, Seq[ByteString]]()  // workerIp -> sampled keys

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
}
