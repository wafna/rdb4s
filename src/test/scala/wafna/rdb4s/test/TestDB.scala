package wafna.rdb4s.test

import wafna.rdb4s.db._
import wafna.rdb4s.dsl._
import scala.concurrent.duration.{FiniteDuration, _}
/**
  * For tests.
  */
object TestDomain {
  // Domain objects.
  case class User(id: Int, name: String)
  case class Company(id: Int, name: String)
}
object TestDB {
  // Database tables.
  private class TUser(alias: String) extends Table("user", alias) {
    val id: TField = field("id")
    val name: TField = field("name")
  }
  private class TCompany(alias: String) extends Table("company", alias) {
    val id: TField = field("id")
    val name: TField = field("name")
  }
  private class TAssociation(alias: String) extends Table("association", alias) {
    val userId: TField = field("user_id")
    val companyId: TField = field("company_id")
  }
  private def checkUpdate(expectedRowCount: Int)(promise: DBPromise[Int]): DBPromise[Unit] =
    promise.map(count => if (count != expectedRowCount) sys error s"Updated $count row(s) but expected $expectedRowCount row(s).")
  trait Crud[T] {
    val selector: Select
    def extractor(rs: RSCursor): T
  }
  // Tables with aliases for the SQL, below.
  private final val u = new TUser("u")
  private final val c = new TCompany("c")
  private final val a = new TAssociation("a")
  /**
    * Projections for domain types.
    */
  object crud {
    import TestDomain._
    object user extends Crud[User] {
      val selector: Select = select(u.id, u.name).from(u)
      def extractor(rs: RSCursor): User = User(rs.int.get, rs.string.get)
    }
    object company extends Crud[Company] {
      val selector: Select = select(c.id, c.name).from(c)
      def extractor(rs: RSCursor): Company = Company(rs.int.get, rs.string.get)
    }
  }
  def apply(database: String, poolName: String, maxSize: Int, idleTimeout: Duration)(
      borrow: TestDB => Unit)(implicit listener: ConnectionPoolListener = ConnectionPoolListenerNOOP): Unit =
    HSQL(database, poolName, maxSize, idleTimeout)(db => borrow(new TestDB(db)))
}
/**
  * Provides a schema and API for a test database.
  */
class TestDB(db: HSQL.DB) {
  import TestDB._
  import TestDomain._
  def createSchema(): DBPromise[Int] = db autoCommit { cx =>
    cx.mutate("DROP TABLE user IF EXISTS", Nil)
    cx.mutate("DROP TABLE company IF EXISTS", Nil)
    cx.mutate(
      """CREATE TABLE user (
        |  id INTEGER IDENTITY PRIMARY KEY,
        |  name VARCHAR(30)
        |)""".stripMargin, Nil)
    cx.mutate(
      """CREATE TABLE company (
        |  id INTEGER IDENTITY PRIMARY KEY,
        |  name VARCHAR(30)
        |)""".stripMargin, Nil)
    cx.mutate(
      """CREATE TABLE association (
        |  user_id INTEGER FOREIGN KEY REFERENCES user (id),
        |  company_id INTEGER FOREIGN KEY REFERENCES company (id)
        |)""".stripMargin, Nil)
  }
  def insertUsers(userNames: Iterable[String]): DBPromise[List[Int]] = db blockCommit { tx =>
    // Do a bunch of stuff in one transaction.
    (userNames map { name =>
      tx.mutate(insert(u)(u.name -> name))
      tx.lastInsertId()
    }).toList
  }
  def insertCompanies(companyNames: Iterable[String]): DBPromise[List[Int]] = db blockCommit { tx =>
    // Do a bunch of stuff in one transaction.
    (companyNames map { name =>
      tx.mutate(insert(c)(c.name -> name))
      tx.lastInsertId()
    }).toList
  }
  def searchUser(s: String): DBPromise[List[User]] = db autoCommit { cx =>
    cx.query(crud.user.selector.where(u.name like s))(crud.user.extractor)
  }
  def searchCompany(s: String): DBPromise[List[Company]] = db autoCommit { cx =>
    cx.query(crud.company.selector.where(c.name like s))(crud.company.extractor)
  }
  // Uses map on promise to add assertion about the result.
  // Best to throw errors there rather than in the connection pool.
  def associate(userId: Int, companyId: Int): DBPromise[Unit] = db blockCommit { tx =>
    tx.mutate(insert(a)(a.userId -> userId, a.companyId -> companyId))
  } map { count => if (1 != count) sys error s"Expected one record update, got $count" else Unit }
  def fetchAssociations(): DBPromise[List[(Int, Int)]] = db autoCommit { cx =>
    cx.query(select(a.userId, a.companyId).from(a))(rs => rs.int.get -> rs.int.get)
  }
  def fetchAssociatedNames(): DBPromise[List[(String, String)]] = db autoCommit { cx =>
    cx.query(select(u.name, c.name).from(a)
        .innerJoin(c).on(a.companyId === c.id)
        .innerJoin(u).on(a.companyId === u.id))(
      rs => (rs.string.get, rs.string.get))
  }
  def updateUser(id: Int, name: String): DBPromise[Unit] = checkUpdate(1)(db autoCommit { cx =>
    cx.mutate(update(u)(u.name -> name).where(u.id === id.q))
  })
  def usersById(ids: List[Int]): DBPromise[List[User]] = db autoCommit { cx =>
    // the descending sort on u.name has no effect; it's here to test orderBy.
    cx.query(crud.user.selector.where(u.id in ids).orderBy(u.id.asc, u.name.desc).sql)(crud.user.extractor)
  }
  def _tester_1(dt: FiniteDuration): DBPromise[(Long, String)] = db autoCommit { _ =>
    Thread sleep dt.toMillis
    Thread.currentThread().getId -> Thread.currentThread().getName
  }
  def _echo[T](t: T): DBPromise[T] = db.autoCommit(_ => t)
}
