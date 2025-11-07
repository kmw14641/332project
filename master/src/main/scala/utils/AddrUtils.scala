package utils

import java.net.InetAddress

object AddrUtils {
  def getAddr: Option[String] = scala.util.Try(InetAddress.getLocalHost.getHostAddress).toOption
}
