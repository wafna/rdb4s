package wafna.rdb4s.test.audit
import java.util.UUID
import wafna.rdb4s.db.{ConnectionPool, RowCursor}
import wafna.rdb4s.db.RDB.DBPromise
import wafna.rdb4s.dsl.{Table, _}
import wafna.rdb4s.test.HSQL
import scala.concurrent.duration._
/** A database consisting of a versioned user table plus an audit table recording
  * changes to the user table (made by users, of course).
  * This design is useful when one needs permanent links to records at a certain time, e.g.
  * the address when something was shipped rather than the current address.
  */
object AuditDB {
  private val timeout: FiniteDuration = 1.second
  val rootUserId = new UUID(0L, 0L)
  val initialVersionId = 0
  /** Makes a database and creates the schema.
    */
  def apply(borrow: AuditDB => Unit): Unit = {
    val dbConfig = new ConnectionPool.Config().name("nested-set")
        .connectionTestTimeout(0).connectionTestCycleLength(1.hour)
        .idleTimeout(1.hour).maxQueueSize(1000).maxPoolSize(4).minPoolSize(1)
    HSQL(s"nested-set-${UUID.randomUUID().toString}", dbConfig) { db =>
      db blockCommit { tx =>
        Array(
          // `record_id` is strictly redundant because it uniquely implies `(entity_id, version_id)` but it is
          // more convenient for linking records.
          // `entity_status` indicates deletion and is used like that; queries will filter out disabled records
          // as if they were actually deleted.
          """create table users (
            |  record_id uuid primary key,
            |  entity_id uuid not null,
            |  version_id integer not null,
            |  entity_status boolean not null,
            |  name varchar(32) not null,
            |  constraint users_unique_record unique (entity_id, version_id),
            |)""".stripMargin,
          "create index ix_entity_id ON users (entity_id)",
          """create table users_audit (
            |  record_id uuid not null foreign key references users (record_id),
            |  actor_id uuid not null foreign key references users (record_id),
            |  time_stamp timestamp default now() not null,
            |  constraint users_audit_unique_record unique (record_id)
            |)""".stripMargin
        ).foreach(tx.mutate(_, Nil))
        // Bootstraps all further activity.
        tx.mutate(insert(u)(u.recordId -> rootUserId, u.entityId -> rootUserId, u.versionId -> initialVersionId, u.entityStatus -> true, u.name -> "root"))
      } reflect timeout
      borrow(new AuditDB(db))
    }
  }
  case class AuditPK(recordId: UUID, entityId: UUID, versionId: Int)
  case class AuditRecord(recordId: UUID, actorId: UUID, timeStamp: FiniteDuration)
  case class User(key: AuditPK, name: String)
  /**
    * Expresses the fields required for a table we're auditing.
    */
  trait AuditedTable {
    this: Table =>
    /** The unique identifier of the record.
      * This is directly implied by (entity_id, version_id).
      */
    val recordId: TField = "record_id"
    /** The stable identifier of the entity, e.g. the root user.
      */
    val entityId: TField = "entity_id"
    /** The version of the record, which numbers ascend in time.
      */
    val versionId: TField = "version_id"
    /** Since we can't delete anything.
      * The intent is that this should be treated much like a deleted record and records where this is false
      * should be largely ignored.
      * They would still be maintained as dependencies on other records regardless of status.
      */
    val entityStatus: TField = "entity_status"
  }
  trait AuditTable {
    this: Table =>
    val recordId: TField = "record_id"
    val actorId: TField = "actor_id"
    val timeStamp: TField = "time_stamp"
  }
  class Users(alias: String) extends Table("users", alias) with AuditedTable {
    val name: TField = "name"
  }
  class UsersAudit(alias: String) extends Table("users_audit", alias) with AuditTable
  object u extends Users("u")
  object ua extends UsersAudit("ua")
  // Gets us the maximum version for each entity.
  object m extends SubQuery("m",
    select(u.entityId as "e", u.versionId.max as "v").from(u).groupBy(u.entityId)) {
    val entityId: TField = "e"
    val versionId: TField = "v"
  }
  /** A convenient way to enforce single item lists where we expect unique records.
    */
  def withSingleton[T](entities: List[T]): T = entities match {
    case e :: Nil => e
    case Nil => sys error s"Entity not found."
    case wat => sys error s"Expected single entity, got $wat"
  }
  /** Postfix notation.
    */
  implicit class `singleton list or bust`[T](val items: List[T]) {
    def singleton: T = withSingleton(items)
  }
}
class AuditDB private(db: HSQL.DB) {
  import AuditDB._
  def getRootUser: DBPromise[User] = db blockCommit { tx =>
    tx.query(selectLatestUser.where(u.entityId === rootUserId))(readUser)
        .headOption.getOrElse(sys error "root user not found.")
  }
  /** Record an update to the user table.
    *
    * @return recordId as an implementation convenience.
    */
  private def updateUserAudit(recordId: UUID, userActor: User)(implicit tx: HSQL.Connection): UUID = {
    tx.mutate(insert(ua)(ua.recordId -> recordId, ua.actorId -> userActor.key.recordId))
    recordId
  }
  /** Create a new user.
    *
    * @param name Must not be held by the most recent version of any entity.
    * @return The new entity id.
    */
  def createUser(name: String)(implicit userActor: User): DBPromise[UUID] = db blockCommit { implicit tx =>
    // We must first check that the name is not in use, i.e. is the name of the latest version of an active entity.
    // Names used in non-latest records are considered discarded and, therefore, available.
    // todo switch to an existence test rather than returning records.
    val collisions = tx.query(select(u.recordId, u.entityId, u.versionId).from(u)
        .innerJoin(m).on((m.entityId === u.entityId) && (m.versionId === u.versionId))
        .where(u.name === name))(r => (r.uuid.get, r.uuid.get, r.int.get))
    if (collisions.nonEmpty) {
      sys error s"User name '$name' exists: $collisions"
    } else {
      val entityId = UUID.randomUUID()
      val recordId = UUID.randomUUID()
      tx.mutate(insert(u)(u.recordId -> recordId, u.entityId -> entityId, u.versionId -> initialVersionId, u.entityStatus -> true, u.name -> name))
      updateUserAudit(recordId, userActor)
      entityId
    }
  }
  /** @return The new record id.
    */
  def updateUser(entityId: UUID, name: String)(implicit userActor: User): DBPromise[UUID] = db blockCommit { implicit tx =>
    val e = tx.query(selectLatestUser.where(u.entityId === entityId))(readUser).singleton
    val recordId = UUID.randomUUID()
    tx.mutate(insert(u)(u.recordId -> recordId, u.entityId -> entityId, u.versionId -> (e.key.versionId + 1), u.entityStatus -> true, u.name -> name))
    updateUserAudit(recordId, userActor)
  }
  /** @return The new record id.
    */
  def deleteUser(entityId: UUID)(implicit userActor: User): DBPromise[UUID] = db blockCommit { implicit tx =>
    val e = tx.query(selectLatestUser.where(u.entityId === entityId))(readUser).singleton
    val recordId = UUID.randomUUID()
    tx.mutate(insert(u)(u.recordId -> recordId, u.entityId -> entityId, u.versionId -> (e.key.versionId + 1), u.entityStatus -> false, u.name -> e.name))
    updateUserAudit(recordId, userActor)
  }
  def getUsers(entityIds: List[UUID]): DBPromise[List[User]] = db autoCommit { tx =>
    tx.query(selectLatestUser.where(u.entityId in entityIds))(readUser)
  }
  private val selectLatestUser: Select =
    select(u.recordId, u.entityId, u.versionId, u.name)
        .from(u)
        .innerJoin(m).on((u.entityId === m.entityId) && (u.versionId === m.versionId))
        .where(u.entityStatus === true)
  private def readUser(r: RowCursor): User =
    User(AuditPK(r.uuid.get, r.uuid.get, r.int.get), r.string.get)
  /** Dumps the entire user table.
    */
  def dumpUsers(): DBPromise[List[(User, Boolean)]] = db autoCommit { tx =>
    tx.query(select(u.recordId, u.entityId, u.versionId, u.name, u.entityStatus).from(u)
    )(r => readUser(r) -> r.bool.get)
  }
  /** Dumps the entire user table.
    */
  def auditUser(entityId: UUID): DBPromise[List[AuditRecord]] = db autoCommit { implicit tx =>
    tx.query(
      select(ua.recordId, ua.actorId, ua.timeStamp).from(ua).innerJoin(u).on(u.recordId === ua.recordId)
          .where(u.entityId === entityId).orderBy(ua.timeStamp.desc))(
      r => AuditRecord(r.uuid.get, r.uuid.get, r.timestamp.get.getTime.millis))
  }
  /** Dumps the entire user audit table.
    */
  def dumpAudit(): DBPromise[List[AuditRecord]] = db autoCommit { tx =>
    tx.query(
      select(ua.recordId, ua.actorId, ua.timeStamp).from(ua).innerJoin(u).on(u.recordId === ua.recordId).orderBy(ua.timeStamp.desc))(
      r => AuditRecord(r.uuid.get, r.uuid.get, r.timestamp.get.getTime.millis))
  }
  /** Dumps the entire user audit table.
    */
  def dumpAudit2(): DBPromise[List[(AuditRecord, User, Boolean)]] = db autoCommit { tx =>
    tx.query(
      select(ua.recordId, ua.actorId, ua.timeStamp, u.recordId, u.entityId, u.versionId, u.name, u.entityStatus)
          .from(ua)
          .innerJoin(u).on(u.recordId === ua.recordId)
          .orderBy(u.entityId.asc, ua.timeStamp.asc))(
      r => (AuditRecord(r.uuid.get, r.uuid.get, r.timestamp.get.getTime.millis), readUser(r), r.bool.get))
  }
}