package wafna.rdb4s.test.cp
import org.scalatest.FlatSpec
import wafna.rdb4s.db._
import wafna.rdb4s.test.TestDB
import scala.concurrent.duration._
/**
  * One big test so that we can create a schema.
  */
class TestConnectionPoolA extends FlatSpec {
  "connection pool" should "pass a bunch of tests" in {
    implicit val listener: ConnectionPoolListener = ConnectionPoolListenerNOOP
    val poolSize = 3
    TestDB(getClass.getCanonicalName, "hdb", poolSize, 1.second) { db =>
      db.createSchema() reflect 1.second
      db.insertUsers(List("Flippy", "Hambone")) reflect 1.second
      assertThrows[CPTimeoutException](db.usersById(List(0)) reflect 1.millis)
      Iterator
          .continually(db.usersById(List(0)))
          .take(100)
          .foreach(r => assertResult(1)((r reflect 1.second).size))
    }
  }
}
