package common.utils

import java.net.InetAddress

object SystemUtils {
  def getLocalIp: String = InetAddress.getLocalHost.getHostAddress

  def getRamMb: Long = {
    val runtime = Runtime.getRuntime
    val maxMemory = runtime.maxMemory()
    maxMemory / (1024 * 1024)
  }

  def getProcessorNum: Int = Runtime.getRuntime.availableProcessors()
}
