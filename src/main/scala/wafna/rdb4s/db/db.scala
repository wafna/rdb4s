package wafna.rdb4s
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
}
