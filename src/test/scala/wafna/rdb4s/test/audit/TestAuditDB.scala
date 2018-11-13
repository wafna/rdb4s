package wafna.rdb4s.test.audit
import java.util.UUID

import org.scalatest.FlatSpec
import wafna.rdb4s.db.CPException
import wafna.rdb4s.db.RDB.DBPromise
import wafna.rdb4s.test.audit.AuditDB.AuditPK

import scala.concurrent.duration._
object TestAuditDB {
  implicit class `default timeout`[T](val p: DBPromise[T]) {
    def timeout: T = p reflect 10.second
  }
}
class TestAuditDB extends FlatSpec {
  import TestAuditDB._
  "audit db" should "work and stuff" in {
    AuditDB { db =>
      implicit val rootUser: AuditDB.User = db.getRootUser.timeout
      assertResult(0)(rootUser.key.versionId)
      val rabbitId: UUID = db.createUser("Binky").timeout
      // db.dumpUsers().timeout foreach println
      val rabbit = db.getUsers(List(rabbitId)).timeout.head
      assertResult(rabbitId)(rabbit.key.entityId)
      assertResult(0)(rabbit.key.versionId)
      db.auditUser(rabbitId).timeout match {
        case r :: Nil =>
          assert(r.actorId == rootUser.key.entityId)
        case _ =>
          fail("Expected one record.")
      }
      db.updateUser(rabbitId, "Bongo").timeout
      val rabbit2 = db.getUsers(List(rabbitId)).timeout.head
      assertResult(rabbitId)(rabbit2.key.entityId)
      assertResult(1)(rabbit2.key.versionId)
      db.auditUser(rabbitId).timeout match {
        case p :: q :: Nil =>
          assert(p.actorId == rootUser.key.entityId)
          assert(q.actorId == rootUser.key.entityId)
        case _ =>
          fail("Expected two records.")
      }
      // 'Binky' is acceptable here because it's a discarded name.
      val u1 = db.createUser("Binky").timeout
      db.getUsers(List(rabbitId)).timeout match {
        case u :: Nil =>
          assertResult(rabbitId)(u.key.entityId)
        case w => fail(s"Expected one record, got $w")
      }
      // 'Bongo' is rejected here because it's the latest in use.
      intercept[CPException.Reflected](db.createUser("Bongo").timeout)
      db.deleteUser(rabbitId).timeout
      assertResult(Nil)(db.getUsers(List(rabbitId)).timeout)
      // We don't get the deleted user.
      assertResult(2)(db.getUsers(List(rootUser.key.entityId, u1)).timeout.length)
    }
  }
}