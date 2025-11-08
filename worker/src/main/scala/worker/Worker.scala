package worker

import com.google.protobuf.ByteString

// Worker Singleton
object Worker {
  private var masterIp: Option[String] = None
  private var masterPort: Option[Int] = None
  private var inputDirs: Seq[String] = Nil
  private var outputDir: Option[String] = None
  private var assignedRange: Option[(ByteString, ByteString)] = None

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

  def setAssignedRange(rangeStart: ByteString, rangeEnd: ByteString): Unit = this.synchronized {
    assignedRange = Some((rangeStart, rangeEnd))
  }

  def getAssignedRange: Option[(ByteString, ByteString)] = this.synchronized {
    assignedRange
  }
}
