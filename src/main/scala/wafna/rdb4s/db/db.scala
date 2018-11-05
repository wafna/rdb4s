package wafna.rdb4s
import wafna.rdb4s.db.RDB.DBPromise
package object db {
  type JDBCConnection = java.sql.Connection
  implicit class `Option to Null`[T](o: Option[T])(implicit _t: Manifest[T]) {
    def orNull: Any = o.getOrElse(Null[T]())
  }
  /**
    * For interpolating NULL into a prepared statement.
    */
  case class Null[T]()(implicit _t: Manifest[T]) {
    val t: Manifest[T] = _t
  }
  /**
    * Expresses the conversion of a row in a result set to a value.
    */
  type Extraction[T] = RSCursor => T
  /**
    * Checks the number of records affected by an update (postfix notation).
    */
  implicit class `check affected record count`(val promise: DBPromise[Int]) {
    def checkRecordCount(expectedRowCount: Int): DBPromise[Unit] =
      promise.map(count => if (count != expectedRowCount) sys error s"Updated $count row(s) but expected $expectedRowCount row(s).")
  }
  /**
    * Checks the number of records affected by an update (prefix notation).
    */
  def checkRecordCount(expectedRowCount: Int)(promise: DBPromise[Int]): DBPromise[Unit] =
    promise checkRecordCount expectedRowCount
}
