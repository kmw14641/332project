package common.utils

import java.net.InetAddress

object SystemUtils {
  def getLocalIp: Option[String] = scala.util.Try(InetAddress.getLocalHost.getHostAddress).toOption
}
