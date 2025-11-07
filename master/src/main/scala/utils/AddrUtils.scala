package utils

import java.net.InetAddress
import scala.util.Try

object AddrUtils {
  def getAddr: Option[String] = Try(InetAddress.getLocalHost.getHostAddress).toOption
}
