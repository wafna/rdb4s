package wafna.rdb4s.test.cp
import org.scalatest.FlatSpec
import wafna.rdb4s.db.{ConnectionPool, ConnectionPoolListener}
import wafna.rdb4s.test.HSQL
import scala.concurrent.duration._
class TestSQL2 extends FlatSpec{
  "dsl-HSQL" should "create valid sql" in {
    import wafna.rdb4s.dsl._
    val cpConfig = new ConnectionPool.Config()
        .name("hdb")
        .maxPoolSize(1)
        .idleTimeout(1.second)
        .connectionTestCycleLength(2.second)
        .connectionTestTimeout(1)
        .maxQueueSize(1000)
    class TWidget(alias: String) extends Table("widget", alias) {
      val id: TField = "id"
      val name: TField = "name"
      val active: TField = "active"
      val effectiveDate: TField = "effective_date"
    }
    val w = new TWidget("w")
    HSQL(getClass.getCanonicalName, cpConfig) { db =>
      db blockCommit { tx =>
        tx.mutate(
          """CREATE TABLE widget (
            |  id INTEGER IDENTITY PRIMARY KEY,
            |  name VARCHAR(30) NOT NULL,
            |  active BOOLEAN NOT NULL,
            |  effective_date BIGINT
            |)""".stripMargin)
        tx.mutate(insert(w)((w.name, "thingy") ? (w.active, true) ? (w.effectiveDate, System.currentTimeMillis())))
      } reflect 1.second
      assertResult(1)((db autoCommit {
        _.query(
          select(w.id, w.name, w.active, w.effectiveDate).from(w).where(w.name === "thingy"))(
          r => (r.int.get, r.string.get, r.bool.get, r.long.get))
      } reflect 1.second).length)
    }(ConnectionPoolListener)
  }
}
