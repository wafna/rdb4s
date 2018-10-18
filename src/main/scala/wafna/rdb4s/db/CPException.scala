package wafna.rdb4s.db
import scala.concurrent.duration.FiniteDuration
abstract class CPException(msg: String, cause: Throwable) extends Exception(msg, cause) {
  def this(msg: String) = this(msg, null)
  def this(cause: Throwable) = this(null, cause)
}
object CPException {
  /**
    * Points to where the promise is awaited along with the root cause.
    */
  class Reflected(e: Throwable)
      extends CPException(e)
  /**
    * Thrown by DBPromise when the timeout has expired.
    */
  class Timeout(timeout: FiniteDuration)
      extends CPException(s"Timeout exceeded: $timeout")
  /**
    * Thrown by ConnectionPool when the queue is full.
    */
  class TaskRejected(msg: String)
      extends CPException(msg)
  /**
    * Thrown by query and mutate in response to any exception thrown by the driver.
    *
    * @param sql The SQL being executed.
    */
  class SQLExecution(sql: String, cause: Throwable)
      extends CPException(sql, cause)
}
