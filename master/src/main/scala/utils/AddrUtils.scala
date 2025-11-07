package utils

import java.net.InetAddress
import java.net.UnknownHostException

object AddrUtils {
  def getAddr: Option[String] = {
    try {
      val addr = InetAddress.getLocalHost.getHostAddress
      Some(addr)
    } catch {
      case _: UnknownHostException => None
    }
  }
}
