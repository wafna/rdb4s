package wafna.rdb4s.db
import scala.concurrent.duration.Duration
/**
  * Listens to events in the connection pool.  Used for gathering metrics.
  * These methods are called in line rather than being driven off a message bus
  * so make it quick.
  */
class ConnectionPoolListener {
  def poolStart(): Unit = Unit
  def poolStop(queueSize: Int): Unit = Unit
  def taskStart(queueSize: Int, timeInQueue: Duration): Unit = Unit
  def taskStop(queueSize: Int, timeToExecute: Duration): Unit = Unit
  def threadStart(threadPoolSize: Int): Unit = Unit
  def threadStop(threadPoolSize: Int): Unit = Unit
}
/**
  * Static instance for implicit defaults.
  */
object ConnectionPoolListener extends ConnectionPoolListener