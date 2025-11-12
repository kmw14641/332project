package common.utils

import java.net.InetAddress

object SystemUtils {
  def getLocalIp: Option[String] = scala.util.Try(InetAddress.getLocalHost.getHostAddress).toOption

  def getRamMb: Long = {
    val runtime = Runtime.getRuntime
    val maxMemory = runtime.maxMemory()
    maxMemory / (1024 * 1024)
  }
}
