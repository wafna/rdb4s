package wafna.rdb4s.test.cp
import org.scalatest.FlatSpec
import wafna.rdb4s
import wafna.rdb4s.db.{ConnectionPool, CPException}
import wafna.rdb4s.test.TestDB
import wafna.rdb4s.test.TestDomain.{Company, User}
import scala.concurrent.duration._
class TestTestDB extends FlatSpec {
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
        // 20ms should be enough; the point is to ensure we didn't wait for the task to actually execute.
        assert(t0 + 20 >= System.currentTimeMillis())
      } { _ =>
        assertThrows[CPException.Timeout](
          db._tester_1(1.second) reflect 0.millis)
      }).take(100) foreach identity
    }
  }
}
