package worker

// Worker Singleton
object Worker {
  val firstMergeDir: String = "/tmp/first_merge"
  val shuffleDir: String = "/tmp/shuffle"

  private var masterIp: Option[String] = None
  private var masterPort: Option[Int] = None
  private var inputDirs: Seq[String] = Nil
  private var outputDir: Option[String] = None
  private var workerInfo: List[(String, Int)] = Nil

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

  def setWorkerInfo(infos: List[(String, Int)]): Unit = this.synchronized {
    workerInfo = infos
  }

  def getWorkerInfo: List[(String, Int)] = this.synchronized {
    workerInfo
  }

  def setOutputDir(dir: String): Unit = this.synchronized {
    outputDir = Some(dir)
  }

  def getOutputDir: Option[String] = this.synchronized {
    outputDir
  }
}
