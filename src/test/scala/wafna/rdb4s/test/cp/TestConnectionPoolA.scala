package wafna.rdb4s.test.cp
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.FlatSpec
import wafna.rdb4s.db._
import wafna.rdb4s.test.TestDB

import scala.concurrent.duration._
/**
  * One big test so that we can create a schema.
  */
class TestConnectionPoolA extends FlatSpec {
  "connection pool" should "pass a bunch of tests" in {
    val cpConfig = new ConnectionPool.Config().name("hdb").maxPoolSize(3).idleTimeout(1.second).maxQueueSize(10)
    TestDB(getClass.getCanonicalName, cpConfig) { db =>
      db.createSchema() reflect 1.second
      db.insertUsers(List("Flippy", "Hambone")) reflect 1.second
      assertThrows[CPTimeoutException](db.usersById(List(0)) reflect 1.millis)
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
    val cpConfig = new ConnectionPool.Config().name("hdb")
        .maxPoolSize(3).minPoolSize(3)
        .idleTimeout(1.second).maxQueueSize(10)
    TestDB(getClass.getCanonicalName, cpConfig) { db =>
      Thread sleep 250
      assertResult(3)(threads.get())
    }
  }
}
