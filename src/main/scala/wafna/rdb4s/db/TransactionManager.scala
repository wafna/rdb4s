package wafna.rdb4s.db
import wafna.rdb4s.bracket
import wafna.rdb4s.db.RDB.Connection
/**
  * This forces clients to declare explicitly how they want transactions to be managed.
  */
class TransactionManager[+R <: Connection](connection: R) {
  // This ensures autoCommit is left the way it started, which is probably over kill.
  @inline private def bracketAutoCommit[T](use: JDBCConnection => T): T = {
    val cx: JDBCConnection = connection.connection
    bracket(cx.getAutoCommit)(cx.setAutoCommit)(_ => use(cx))
  }
  def autoCommit[T](use: R => T): T = bracketAutoCommit { cx =>
    cx setAutoCommit true
    use(connection)
  }
  def blockCommit[T](use: R => T): T = bracketAutoCommit { cx =>
    cx setAutoCommit false
    try {
      val t: T = use(connection)
      cx.commit()
      t
    } catch {
      case e: Throwable =>
        cx.rollback()
        throw e
    }
  }
}
