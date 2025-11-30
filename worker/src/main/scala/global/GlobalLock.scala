package global

object GlobalLock {
  val diskIoLock = new Object()
}