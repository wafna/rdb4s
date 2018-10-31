package wafna.rdb4s.test.dsl
import org.scalatest.FlatSpec
import wafna.rdb4s
import wafna.rdb4s.db._
import wafna.rdb4s.test.{HSQL, TestDB}
import wafna.rdb4s.test.TestDomain.{Company, User}
import scala.concurrent.duration._
class TestDSL extends FlatSpec {
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
  "dsl-TestDB" should "create valid sql" in {
    val cpConfig = new ConnectionPool.Config()
        .name("hdb")
        .maxPoolSize(1)
        .idleTimeout(10.second)
        .connectionTestCycleLength(2.second)
        .connectionTestTimeout(1)
        .maxQueueSize(1000)
    TestDB(cpConfig) { db =>
      db.createSchema() reflect 1.second
      // Some data.  We'll use the fact that the array index and the entity id will be identical.
      val userNames = Array("Bob", "Carol", "Ted", "Alice")
      val companyNames = Array("Alpha", "Bravo", "Charlie", "Delta")
      assertResult(
        userNames.indices.toList,
        "generates the expected ids")(
        db.insertUsers(userNames) reflect 1.second)
      assertResult(
        companyNames.indices.toList,
        "generates the expected ids")(
        db.insertCompanies(companyNames) reflect 1.second)
      assertResult(
        List(User(0, userNames(0)), User(1, userNames(1))),
        "finds the expect entities by partial name match.")(
        db.searchUser("%o%") reflect 1.second)
      assertResult(
        List(Company(1, companyNames(1))),
        "finds the expect entities by partial name match.")(
        db.searchCompany("%o%") reflect 1.second)
      // Validates that we reflect the exceptions and that the originating exceptions exist.
      assertResult(
        classOf[java.sql.SQLIntegrityConstraintViolationException],
        "Foreign key violation..")(
        intercept[CPException.Reflected](
          // The real cause is two levels deep. The next level down contains the SQL.
          db.associate(99, 99) reflect 10.millis).getCause.getCause.getClass)
      // A premise of the joins, below.
      db.associate(0, 0) reflect 10.millis
      assertResult(
        List(0 -> 0))(
        db.fetchAssociations() reflect 1.second)
      assertResult(
        List(userNames(0) -> companyNames(0)))(
        db.fetchAssociatedNames() reflect 1.second)
      assertResult(
        userNames.zipWithIndex.toList.map(r => User(r._2, r._1)))(
        db.usersById(userNames.indices.toList) reflect 1.second)
      val newName = "Sherlock"
      db.updateUser(0, newName) reflect 1.second
      assertResult(List(User(0, newName)))(db.usersById(List(0)) reflect 1.second)
      // Test that the timeout mechanism works.
      Iterator.continually(rdb4s.bracket(System.currentTimeMillis()) { t0 =>
        // Asserts that when we timeout we didn't take much longer than the indicated time limit.
        // 10ms should be way more than enough; the point is to ensure we didn't wait for the task to
        // actually execute.
        assert(t0 + 10 >= System.currentTimeMillis())
      } { _ =>
        assertThrows[CPException.Timeout](
          db._tester_1(1.second) reflect 0.millis)
      }).take(100) foreach identity
    }
  }
  "dsl" should "emit literal sql" in {
    import rdb4s.dsl._
    class Thing(alias: String) extends Table("something", alias) {
      val id: TField = "id"
      val name: TField = "name"
      val rank: TField = "rank"
      val serialNumber: TField = "serial_number"
    }
    implicit class Straighten(s: String) {
      def straighten(): String = s.replaceAll("[\r\n]+", " ").replaceAll("\\s+", " ").trim
    }
    object thing1 extends Thing("a")
    object thing2 extends Thing("b")
    object thing3 extends Thing("c")
    assertResult(
      """SELECT a.id, a.name, b.id, b.name
        |FROM something a
        |INNER JOIN something b ON (a.id = b.id)
        |WHERE (a.id = ?)""".stripMargin.straighten())(
      select(thing1.id, thing1.name, thing2.id, thing2.name).from(thing1)
          .innerJoin(thing2).on(thing1.id === thing2.id)
          .where(thing1.id === 11)._1.straighten())
    assertResult(
      """SELECT a.id, a.name, a.rank, a.serial_number
        |FROM something a
        |INNER JOIN something b ON (a.id = b.id)
        |INNER JOIN something c ON (c.id = b.id)
        |WHERE (a.id = ?)""".stripMargin.straighten())(
      select(thing1.id, thing1.name, thing1.rank, thing1.serialNumber).from(thing1)
          .innerJoin(thing2).on(thing1.id === thing2.id)
          .innerJoin(thing3).on(thing3.id === thing2.id)
          .where(thing1.id === 11)._1.straighten())
    assertResult(
      """SELECT a.id, a.name, a.rank, a.serial_number
        |FROM something a
        |INNER JOIN something b ON (a.id = b.id)
        |INNER JOIN something c ON (c.id = b.id)
        |WHERE ((a.id = ?) AND (b.id = ?))""".stripMargin.straighten())(
      select(thing1.id, thing1.name, thing1.rank, thing1.serialNumber).from(thing1)
          .innerJoin(thing2).on(thing1.id === thing2.id)
          .innerJoin(thing3).on(thing3.id === thing2.id)
          .where((thing1.id === 11) && (thing2.id === 99))._1.straighten())
  }
}
