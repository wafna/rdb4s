package wafna.rdb4s.test.cp
import org.scalatest.FlatSpec
import wafna.rdb4s.db.{ConnectionPoolListener, ConnectionPoolListenerNOOP, CPTimeoutException}
import wafna.rdb4s.test.TestDB

import scala.concurrent.duration._
class TestConnectionPool extends FlatSpec {
  object ConnectionPoolListenerConsole extends ConnectionPoolListener {
    private def spew(s: String): Unit = println(s"--- $s")
    def poolStart(): Unit =
      spew("pool start")
    def poolStop(queueSize: Int): Unit =
      spew(s"pool stop: queue size = $queueSize")
    def taskStart(queueSize: Int, timeInQueue: Duration): Unit =
      spew(s"task start: queue size = $queueSize, wait time: $timeInQueue, thread = ${Thread.currentThread().getName}")
    def taskStop(queueSize: Int, timeToExecute: Duration): Unit =
      spew(s"task stop: queue size = $queueSize, wait time: $timeToExecute, thread = ${Thread.currentThread().getName}")
    def threadStart(threadPoolSize: Int): Unit =
      spew(s"thread start: pool size = $threadPoolSize, thread = ${Thread.currentThread().getName}")
    def threadStop(threadPoolSize: Int): Unit =
      spew(s"thread stop: pool size = $threadPoolSize, thread = ${Thread.currentThread().getName}")
  }
  // This test never submits any tasks.
  "connection pool" should "shut down cleanly immediately" in {
    implicit val listener: ConnectionPoolListener = ConnectionPoolListenerNOOP
    Array(1, 3, 20) foreach { poolSize =>
      assertThrows[RuntimeException](TestDB("sdbc", "hdb", poolSize, 1.second) { _ => sys error "Barf!" })
    }
  }
  "connection pool" should "shut down cleanly if the borrower barfs on timeout" in {
    implicit val listener: ConnectionPoolListener = ConnectionPoolListenerNOOP
    Array(1, 3, 20) foreach { poolSize =>
      TestDB(getClass.getCanonicalName, "hdb", poolSize, 1.second) { db =>
        assertThrows[CPTimeoutException](db._tester_1(100.millis) reflect 10.millis)
      }
    }
  }
  "connection pool" should "run exactly the maximum number of threads when saturated" in {
    implicit val listener: ConnectionPoolListener = ConnectionPoolListenerNOOP
    val timeout = 500.millis
    Array(1, 3, 20) foreach { poolSize =>
      TestDB(getClass.getCanonicalName, "hdb", poolSize, 1.second) { db =>
        assertResult(poolSize)(
          Iterator
              // the duration should be long enough that we can submit them fast enough.
              .continually(db._tester_1(10.millis))
              // submit a whole bunch just to make sure
              .take(10 * poolSize)
              .map(_ reflect timeout)
              .foldLeft(Set[String]())(_ + _._2).size)
      }
    }
  }
}
