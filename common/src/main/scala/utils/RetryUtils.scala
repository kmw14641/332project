package common.utils

import scala.concurrent.{Future, blocking, ExecutionContext}

object RetryUtils {
  val maxTries = 10
  
  def retry[T](operation: => Future[T], tries: Int = 1)(implicit ec: ExecutionContext): Future[T] = {
    operation.recoverWith {
      case _ if tries < maxTries =>
        println(s"Retrying, attempt #$tries")
        blocking { Thread.sleep(math.pow(2, tries).toLong * 1000) }
        retry(operation, tries + 1)
    }
  }
}