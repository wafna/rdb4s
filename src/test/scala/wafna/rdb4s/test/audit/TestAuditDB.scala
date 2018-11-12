package wafna.rdb4s.test.audit
import org.scalatest.FlatSpec
import wafna.rdb4s.db.RDB.DBPromise
import scala.concurrent.duration._
object TestAuditDB {
  implicit class `default timeout`[T](val p: DBPromise[T]){
    def timeout: T = p reflect 1.second
  }
}
class TestAuditDB extends FlatSpec {
  import TestAuditDB._
  "audit db" should "work and stuff" in {
    AuditDB { db =>
      implicit val rootUser: AuditDB.User = db.getRootUser.timeout
      println(rootUser)
      val rabbitId = db.createUser("Binky").timeout
      println(rabbitId)
      db.updateUser(rabbitId, "Bongo").timeout
      db.auditUser(rabbitId).timeout foreach println
      println(db.getUser(rabbitId).timeout)
      db.dumpUsers().timeout foreach println
      println("-------")
      db.wat().timeout foreach println
    }
  }
}