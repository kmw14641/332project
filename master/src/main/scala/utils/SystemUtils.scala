package utils

import java.net.InetAddress

object SystemUtils {
  def getAddr: Option[String] = scala.util.Try(InetAddress.getLocalHost.getHostAddress).toOption
}
