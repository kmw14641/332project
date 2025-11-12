package master

import master.MasterService.WorkerInfo

// Master Singleton
object Master {
  private var workersNum: Int = -1

  def setWorkersNum(num: Int): Unit = this.synchronized {
    workersNum = num
  }

  def getWorkersNum: Int = this.synchronized {
    workersNum
  }
}
