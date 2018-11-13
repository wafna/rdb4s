package wafna.rdb4s.test.audit
import java.util.UUID
import wafna.rdb4s.db.{ConnectionPool, RowCursor}
import wafna.rdb4s.db.RDB.DBPromise
import wafna.rdb4s.dsl.{Table, _}
import wafna.rdb4s.test.HSQL
import scala.concurrent.duration._
object AuditDB {
  private val timeout: FiniteDuration = 1.second
  /**
    * We don't care where the serial ids of the tables start but, for users, we can stipulate the very first
    * entity id.
    */
  val rootUserId = 0
  val initialVersionId = 0
  /**
    * Makes a database and creates the schema.
    */
  def apply(borrow: AuditDB => Unit): Unit = {
    val dbConfig = new ConnectionPool.Config().name("nested-set")
        .connectionTestTimeout(0).connectionTestCycleLength(1.hour)
        .idleTimeout(1.hour).maxQueueSize(1000).maxPoolSize(4).minPoolSize(1)
    HSQL(s"nested-set-${UUID.randomUUID().toString}", dbConfig) { db =>
      db blockCommit { tx =>
        Array(
          // It's problematic to put constraints on name, here.
          // If we make it unique by entity it prevents disused names from ever being used.
          // What is wanted is for names to be unique among most recent versions.
          """create table users (
            |  record_id integer identity primary key,
            |  entity_id integer not null,
            |  version_id integer not null,
            |  entity_status boolean not null,
            |  name varchar(32) not null,
            |  constraint users_unique_record unique (entity_id, version_id)
            |)""".stripMargin,
          """create table users_audit (
            |  record_id integer not null foreign key references users (record_id),
            |  actor_id integer not null foreign key references users (record_id),
            |  time_stamp timestamp default now() not null,
            |  constraint users_audit_unique_record unique (record_id)
            |)""".stripMargin
        ).foreach(tx.mutate(_, Nil))
        // Boot straps all further activity.
        tx.mutate(insert(u)(u.entityId -> rootUserId, u.versionId -> initialVersionId, u.entityStatus -> true, u.name -> "root"))
      } reflect timeout
      borrow(new AuditDB(db))
    }
  }
  case class AuditPK(recordId: Int, entityId: Int, versionId: Int)
  case class AuditRecord(recordId: Int, actorId: Int, timeStamp: FiniteDuration)
  case class User(key: AuditPK, name: String)
  /**
    *
    */
  trait AuditedTable {
    this: Table =>
    /**
      * The unique identifier of the record.
      * This is directly implied by (entity_id, version_id).
      */
    val recordId: TField = "record_id"
    /**
      * The stable identifier of the entity, e.g. the root user.
      */
    val entityId: TField = "entity_id"
    /**
      * The version of the record, which numbers ascend in time.
      */
    val versionId: TField = "version_id"
    /**
      * Since we can't delete anything.
      * The intent is that this should be treated much like a deleted record and records where this is false
      * should be largely ignored.
      * They would still be maintained as dependencies on other records regardless of status.
      */
    val entityStatus: TField = "entity_status"
  }
  class Users(alias: String) extends Table("users", alias) with AuditedTable {
    val name: TField = "name"
  }
  trait AuditTable {
    this: Table =>
    val recordId: TField = "record_id"
    val actorId: TField = "actor_id"
    val timeStamp: TField = "time_stamp"
  }
  class UsersAudit(alias: String) extends Table("users_audit", alias) with AuditTable
  object u extends Users("u")
  object ua extends UsersAudit("ua")
  // Gets us the maximum version for each entity.
  object p extends SubQuery("p",
    select(u.entityId as "e", u.versionId.max as "v").from(u).groupBy(u.entityId)) {
    val entityId: TField = "e"
    val versionId: TField = "v"
  }
}
class AuditDB private(db: HSQL.DB) {
  import AuditDB._
  def getRootUser: DBPromise[User] = db blockCommit { tx =>
    tx.query(
      select(u.recordId, u.entityId, u.versionId.max, u.name)
          .from(u).where(u.entityId === rootUserId)
          .groupBy(u.entityId, u.recordId))(
      r => User(AuditPK(r.int.get, r.int.get, r.int.get), r.string.get))
        .headOption.getOrElse(sys error "root user not found.")
  }
  private def updateAuditUser(recordId: Int, userActor: User)(implicit tx: HSQL.Connection): Int = {
    tx.mutate(insert(ua)(ua.recordId -> recordId, ua.actorId -> userActor.key.recordId))
    recordId
  }
  def auditUser(entityId: Int): DBPromise[List[AuditRecord]] = db autoCommit { implicit tx =>
    tx.query(
      select(ua.recordId, ua.actorId, ua.timeStamp).from(ua).innerJoin(u).on(u.recordId === ua.recordId).where(u.entityId === entityId))(
      r => AuditRecord(r.int.get, r.int.get, r.timestamp.get.getTime.millis))
  }
  def createUser(name: String)(implicit userActor: User): DBPromise[Int] = db blockCommit { implicit tx =>
    val collisions = tx.query(select(u.recordId, u.entityId, u.versionId).from(u)
        .innerJoin(p).on((p.entityId === u.entityId) && (p.versionId === u.versionId))
        .where(u.name === name))(r => (r.int.get, r.int.get, r.int.get))
    if (collisions.nonEmpty) {
      sys error s"User name '$name' exists: $collisions"
    } else {
      val entityId = 1 + tx.query(select(u.entityId.max).from(u))(_.int.get).head
      tx.mutate(insert(u)(u.entityId -> entityId, u.versionId -> initialVersionId, u.entityStatus -> true, u.name -> name))
      updateAuditUser(tx.lastInsertId(), userActor)
    }
  }
  def updateUser(entityId: Int, name: String)(implicit userActor: User): DBPromise[Int] = db blockCommit { implicit tx =>
    tx.query(selectLatestUser.where(u.entityId === entityId))(r => (r.int.get, r.int.get, r.int.get, r.string.get)) match {
      case Nil =>
        sys error s"Entity not found: $entityId."
      case h :: _ =>
        tx.mutate(insert(u)(u.entityId -> entityId, u.versionId -> (h._1 + 1), u.entityStatus -> h._2, u.name -> name))
        updateAuditUser(tx.lastInsertId(), userActor)
    }
  }
  def deleteUser(entityId: Int)(implicit userActor: User): DBPromise[Int] = db blockCommit { implicit tx =>
    tx.query(selectLatestUser.where(u.entityId === entityId))(r => (r.int.get, r.int.get, r.int.get, r.string.get)) match {
      case Nil =>
        sys error s"Entity not found: $entityId."
      case h :: _ =>
        tx.mutate(insert(u)(u.entityId -> entityId, u.versionId -> (h._3 + 1), u.entityStatus -> false, u.name -> h._4))
        updateAuditUser(tx.lastInsertId(), userActor)
    }
  }
  def getUsers(entityIds: List[Int]): DBPromise[List[User]] = db autoCommit { tx =>
    tx.query(selectLatestUser.where(u.entityId in entityIds))(readUser)
  }
  private val selectLatestUser: Select =
    select(u.recordId, u.entityId, u.versionId, u.name)
        .from(u)
        .innerJoin(p).on((u.entityId === p.entityId) && (u.versionId === p.versionId))
        .where(u.entityStatus === true)
  private def readUser(r: RowCursor): User =
    User(AuditPK(r.int.get, r.int.get, r.int.get), r.string.get)
  def dumpUsers(): DBPromise[List[(User, Boolean)]] = db autoCommit { tx =>
    tx.query(select(u.recordId, u.entityId, u.versionId, u.name, u.entityStatus).from(u)
    )(r => User(AuditPK(r.int.get, r.int.get, r.int.get), r.string.get) -> r.bool.get)
  }
  def wat(): DBPromise[List[Any]] = db autoCommit { tx =>
    // val entityId = 1
    val m = new Users("m")
    val q1: Select = select(m.entityId as "e", m.versionId.max as "v").from(m).groupBy(m.entityId)
    println(q1.toString)
    tx.query(q1) { r =>
      println(s"${r.int.get}, ${r.int.get}")
    }
    object p extends SubQuery("p", q1) {
      val entityId: TField = "e"
      val versionId: TField = "v"
    }
    val q2 = select(u.recordId, u.entityId, u.versionId, u.name).from(u)
        .innerJoin(p).on((p.entityId === u.entityId) && (p.versionId === u.versionId)).where(u.name === "Bongo")
    tx.query(q2)(r => (r.int.get, r.int.get, r.int.get, r.string.get))
  }
}