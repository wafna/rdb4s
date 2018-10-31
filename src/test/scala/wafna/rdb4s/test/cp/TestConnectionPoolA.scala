package wafna.rdb4s.test.cp
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.FlatSpec
import wafna.rdb4s.db._
import wafna.rdb4s.test.TestDB
import scala.concurrent.duration._
import scala.util.Try
/**
  * One big test so that we can create a schema.
  */
class TestConnectionPoolA extends FlatSpec {
  "connection pool" should "recover after timeout" in {
    val cpConfig = new ConnectionPool.Config().name("hdb").maxPoolSize(3)
        .idleTimeout(1.second)
        .connectionTestCycleLength(2.second)
        .connectionTestTimeout(1)
        .maxQueueSize(10)
    TestDB(cpConfig) { db =>
      db.createSchema() reflect 1.second
      db.insertUsers(List("Flippy", "Hambone")) reflect 1.second
      assertThrows[CPException.Timeout](db.usersById(List(0)) reflect 1.millis)
      Iterator
          .continually(db.usersById(List(0)))
          .take(100)
          .foreach(r => assertResult(1)((r reflect 1.second).size))
    }
  }
  "connection pool" should "maintain a minimum thread pool" in {
    val threads = new AtomicInteger(0)
    implicit val listener: ConnectionPoolListener = new ConnectionPoolListener {
      override def threadStart(threadPoolSize: Int): Unit = {
        threads.incrementAndGet()
      }
    }
    val cpConfig = new ConnectionPool.Config()
        .name("hdb")
        .maxPoolSize(3).minPoolSize(3)
        .idleTimeout(1.second)
        .connectionTestCycleLength(2.second)
        .connectionTestTimeout(1)
        .maxQueueSize(10)
    TestDB(cpConfig) { _ =>
      Thread sleep 250
      assertResult(3)(threads.get())
    }
  }
  "connection pool" should "wait at least the full time before timing out" in {
    def timer[T](t: => T): (T, Long) = {
      val t0 = System.currentTimeMillis()
      val x = t
      (x, System.currentTimeMillis() - t0)
    }
    val cpConfig = new ConnectionPool.Config()
        .name("hdb")
        .maxPoolSize(3).minPoolSize(3)
        .idleTimeout(1.second)
        .connectionTestCycleLength(2.second)
        .connectionTestTimeout(1)
        .maxQueueSize(10)
    TestDB(cpConfig) { db =>
      Array(1, 10, 100, 1000) foreach { timeout =>
        assert(timer(Try(db._tester_1(10.second) take timeout.millis))._2 >= timeout)
      }
    }
  }
}
