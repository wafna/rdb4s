package wafna.rdb4s.test.cp
import org.scalatest.FlatSpec
import wafna.rdb4s.db.ConnectionPool
import wafna.rdb4s.db.RDB.DBPromise
import wafna.rdb4s.test.{ConnectionPoolEventCounter, HSQL}
import scala.concurrent.duration._
class TestSQL extends FlatSpec {
  "rdb" should "execute SQL statements" in {
    implicit val listener: ConnectionPoolEventCounter = new ConnectionPoolEventCounter
    val cpConfig = new ConnectionPool.Config().name("hdb").maxPoolSize(1).idleTimeout(1.second).maxQueueSize(10000)
    ConnectionPool[HSQL.Connection](cpConfig,
      new HSQL.ConnectionManager("sdbc")) { db =>
      val timeout = 500.millis
      assertResult(42)(db.autoCommit(_.query("SELECT ? FROM INFORMATION_SCHEMA.SYSTEM_USERS", List(42))(_.int.get).head) reflect timeout)
      val widgetNames = List("Sprocket", "Cog", "Tappet")
      val value: DBPromise[Seq[Int]] = db blockCommit { tx =>
        tx.mutate(
          """CREATE TABLE widget (
            |  id INTEGER IDENTITY PRIMARY KEY,
            |  name VARCHAR(30)
            |)""".stripMargin, Nil)
        widgetNames map { widget =>
          tx.mutate("""INSERT INTO widget (name) VALUES (?) """, List(widget))
          tx.lastInsertId()
        }
      }
      assertResult(
        List(0, 1, 2), "recover the ids created for the new records")(
        value reflect timeout)
      assertResult(widgetNames.zipWithIndex.map(p => p._2 -> p._1),
        "recover all the widgets that were inserted")(
        db.autoCommit(_.query("SELECT id, name FROM widget ORDER BY id", Nil)(r => (r.int.get, r.string.get))) reflect timeout)
    }
    // ensure all the listener methods got called as expected.
    assertResult(
      ConnectionPoolEventCounter.ConnectionPoolEventCounts(
        poolStart = true, poolStop = true,
        taskStart = 1, taskStop = 1,
        threadStart = 3, threadStop = 3))(
      listener.getCounts)
  }
}