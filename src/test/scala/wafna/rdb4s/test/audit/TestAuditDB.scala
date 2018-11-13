package wafna.rdb4s.test.audit
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
      assertResult(AuditPK(0, 0, 0))(rootUser.key)
      val rabbitId = db.createUser("Binky").timeout
      assertResult(1)(rabbitId)
      val rabbit = db.getUsers(List(rabbitId)).timeout.head
      assertResult(AuditPK(1, rabbitId, 0))(rabbit.key)
      db.auditUser(rabbitId).timeout match {
        case r :: Nil =>
          assert(r.actorId == rootUser.key.entityId)
          assert(r.recordId == 1)
        case _ =>
          fail("Expected one record.")
      }
      db.updateUser(rabbitId, "Bongo").timeout
      db.auditUser(rabbitId).timeout match {
        case p :: q :: Nil =>
          assert(p.actorId == rootUser.key.entityId)
          assert(p.recordId == 1)
          assert(q.actorId == rootUser.key.entityId)
          assert(q.recordId == 2)
        case _ =>
          fail("Expected two records.")
      }
      // 'Binky' is acceptable here because it's a discarded name.
      db.createUser("Binky").timeout
      db.getUsers(List(rabbitId)).timeout match {
        case u :: Nil =>
          assertResult(AuditPK(2,1,1))(u.key)
        case w => fail(s"Expected one record, got $w")
      }
      // 'Bongo' is rejected here because it's the latest in use.
      intercept[CPException.Reflected](db.createUser("Bongo").timeout)
      db.deleteUser(rabbitId).timeout
      assertResult(Nil)(db.getUsers(List(rabbitId)).timeout)
      assertResult(2)(db.getUsers(List(0, 2)).timeout.length)
    }
  }
}