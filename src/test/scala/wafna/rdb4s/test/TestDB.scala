package wafna.rdb4s.test
import wafna.rdb4s.db._
import wafna.rdb4s.db.RDB.DBPromise
import wafna.rdb4s.dsl._
import scala.concurrent.duration.FiniteDuration
/**
  * For tests.
  */
object TestDomain {
  // Domain objects.
  case class User(id: Int, name: String)
  case class Company(id: Int, name: String)
}
object TestDB {
  trait Read[T] {
    def read(rs: RSCursor => T)
  }
  trait ActiveRecord {
    this: Table =>
    val active: TField = "active"
    val effectiveDate: TField = "effective_date"
  }
  // Database tables.
  private class TUser(alias: String) extends Table("user", alias)
      with ActiveRecord {
    val id: TField = "id"
    val name: TField = "name"
  }
  private class TCompany(alias: String) extends Table("company", alias)
      with ActiveRecord {
    val id: TField = "id"
    val name: TField = "name"
  }
  private class TAssociation(alias: String) extends Table("association", alias) {
    val userId: TField = "user_id"
    val companyId: TField = "company_id"
  }
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
  /**
    * Makes a database with a random UUID so that tests can be independent.
    */
  def apply(config: ConnectionPool.Config)(
      borrow: TestDB => Unit)(implicit listener: ConnectionPoolListener = ConnectionPoolListener): Unit =
    HSQL(java.util.UUID.randomUUID().toString, config)(db => borrow(new TestDB(db)))
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
        |  name VARCHAR(30) NOT NULL,
        |  active BOOLEAN NOT NULL,
        |  effective_date BIGINT
        |)""".stripMargin, Nil)
    cx.mutate(
      """CREATE TABLE company (
        |  id INTEGER IDENTITY PRIMARY KEY,
        |  name VARCHAR(30) NOT NULL,
        |  active BOOLEAN NOT NULL,
        |  effective_date BIGINT
        |)""".stripMargin, Nil)
    cx.mutate(
      """CREATE TABLE association (
        |  user_id INTEGER FOREIGN KEY REFERENCES user (id),
        |  company_id INTEGER FOREIGN KEY REFERENCES company (id)
        |)""".stripMargin, Nil)
    cx.mutate(
      """CREATE TABLE stuff (
        |  i INTEGER,
        |  f FLOAT,
        |  d DOUBLE,
        |  b BOOLEAN,
        |  c CHAR(32),
        |  v VARCHAR(32)
        |)""".stripMargin, Nil)
  }
  def insertUsers(userNames: Iterable[String]): DBPromise[List[Int]] = db blockCommit { tx =>
    // Do a bunch of stuff in one transaction.
    (userNames map { name =>
      tx.mutate(insert(u)(Array(u.name -> name, u.active -> true, u.effectiveDate -> System.currentTimeMillis())))
      tx.lastInsertId()
    }).toList
  }
  def insertCompanies(companyNames: Iterable[String]): DBPromise[List[Int]] = db blockCommit { tx =>
    // Do a bunch of stuff in one transaction.
    (companyNames map { name =>
      tx.mutate(insert(c)(Array(c.name -> name, c.active -> true, c.effectiveDate -> System.currentTimeMillis())))
      tx.lastInsertId()
    }).toList
  }
  def searchUser(s: String): DBPromise[List[User]] = db autoCommit { cx =>
    // orderBy ensures we can make predictable assertions about result sets
    cx.query(crud.user.selector.where(u.name like s).orderBy(u.id.asc))(crud.user.extractor)
  }
  def searchCompany(s: String): DBPromise[List[Company]] = db autoCommit { cx =>
    cx.query(crud.company.selector.where(c.name like s))(crud.company.extractor)
  }
  // Uses map on promise to add assertion about the result.
  // Best to throw errors there rather than in the connection pool.
  def associate(userId: Int, companyId: Int): DBPromise[Unit] = db blockCommit { tx =>
    tx.mutate(insert(a)(Array(a.userId -> userId, a.companyId -> companyId)))
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
  def companiesForUsers(userIds: List[Int]): DBPromise[List[(Int, Company)]] = db autoCommit {
    _.query(
      select(u.id, c.id, c.name).from(c).innerJoin(u).on(u.id === c.id).where(u.id in userIds))(
      r => (r.int.get, crud.company.extractor(r)))
  }
  def updateUser(id: Int, name: String): DBPromise[Unit] = db autoCommit { cx =>
    cx.mutate(update(u)(Array(u.name -> name)).where(u.id === id.q))
  } checkAffectedRows 1
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
