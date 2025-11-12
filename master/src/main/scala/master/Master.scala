package master

import master.MasterService.WorkerInfo

// Master Singleton
object Master {
  private var workersNum: Int = -1
  private var registeredWorkers = Map[String, WorkerInfo]()

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
}
