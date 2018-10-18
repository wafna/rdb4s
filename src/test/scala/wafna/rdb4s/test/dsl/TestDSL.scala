package wafna.rdb4s.test.dsl
import org.scalatest.FlatSpec
import wafna.rdb4s.db._
import wafna.rdb4s.test.TestDB
import wafna.rdb4s.test.TestDomain.{Company, User}
import scala.concurrent.duration._
class TestDSL extends FlatSpec {
  "dsl" should "create valid sql" in {
    val cpConfig = new ConnectionPool.Config().name("hdb").maxPoolSize(1).idleTimeout(1.second).maxQueueSize(10)
    TestDB(getClass.getCanonicalName, cpConfig) { db =>
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
        intercept[CPReflectedException](
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
        db.usersById(userNames.indices.toList) reflect 10.millis)
      val newName = "Sherlock"
      db.updateUser(0, newName) reflect 10.millis
      assertResult(List(User(0, newName)))(db.usersById(List(0)) reflect 10.millis)
      // Test that the timeout mechanism works.
      // todo verify that the time elapsed looks like the indicated timeout.
      assertThrows[CPTimeoutException](
        db._tester_1(1.second) reflect 10.millis)
    }
  }
}
