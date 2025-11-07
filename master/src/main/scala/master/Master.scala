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
  
  def registerWorker(request: WorkerInfo): Unit = this.synchronized {
    val workerIp = request.ip
    registeredWorkers += (workerIp -> request)
  }

  def getRegisteredWorkers: Map[String, WorkerInfo] = this.synchronized {
    registeredWorkers.toMap
  }
}
