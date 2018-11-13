package wafna.rdb4s.db
import java.sql.ResultSet
import java.util.UUID

import wafna.rdb4s.bracket

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
abstract class RDB[D <: java.sql.Driver](implicit m: ClassTag[D]) {
  final val driverName: String = m.runtimeClass.getCanonicalName
}
object RDB {
  /**
    * Results deferred to eventual execution in the connection pool.
    */
  abstract class DBPromise[T] {
    self: DBPromise[T] =>
    def take(timeOut: FiniteDuration): Try[T]
    def reflect(timeOut: FiniteDuration): T =
      take(timeOut).transform(Success(_), e => Failure(new CPException.Reflected(e))).get
    def map[R](f: T => R): DBPromise[R] =
      (timeOut: FiniteDuration) => self.take(timeOut).map(f(_))
  }
  object DBPromise {
    def success[T](t: T): DBPromise[T] = _ => Success(t)
    def failure[T](e: Throwable): DBPromise[T] = _ => Failure(e)
  }
  /**
    * Produces vendor specific connections.
    */
  abstract class ConnectionManager[R <: Connection] {
    def createConnection(): R
  }
  /**
    * Checks the number of records affected by an update.
    */
  implicit class `check affected row count`(val promise: DBPromise[Int]) {
    def checkAffectedRows(expectedRowCount: Int): DBPromise[Unit] =
      promise.map(count => if (count != expectedRowCount) sys error s"Updated $count row(s) but expected $expectedRowCount row(s).")
  }
  private object ConnectionRDB4S {
    /**
      * Iterate a ResultSet.
      */
    class RSIterator(rs: ResultSet) extends Iterator[RowCursor]() {
      override def hasNext: Boolean = rs.next()
      override def next(): RowCursor = new RowCursor(rs)
    }
  }
  /**
    * Vendor specific wrapper for a JDBC connection.
    */
  abstract class Connection(private[rdb4s] val connection: JDBCConnection)
      extends AutoCloseable {
    import java.sql.{PreparedStatement, Timestamp}
    import ConnectionRDB4S._
    override protected[rdb4s] def close(): Unit = connection.close()
    // todo use this to test and retain idle connections
    def isValid(timeout: Int): Boolean = connection isValid timeout
    /**
      * @see java.sql.PreparedStatement.executeQuery
      */
    def query[T](sql: String, args: List[Any] = Nil)(extraction: Extraction[T]): List[T] =
      try bracket(prepareStatement(sql, args))(_.close) { stmt =>
        bracket(stmt.executeQuery())(Option(_).foreach(_.close()))(r => new RSIterator(r).map(extraction).toList)
      } catch {
        case e: Throwable => throw new CPException.SQLExecution(sql, e)
      }
    def query[T](q: (String, List[Any])): Extraction[T] => List[T] = query(q._1, q._2)
    /**
      * @see java.sql.PreparedStatement.executeUpdate
      */
    def mutate(sql: String, args: List[Any] = Nil): Int =
      try bracket(prepareStatement(sql, args))(_.close())(_.executeUpdate())
      catch {
        case e: Throwable => throw new CPException.SQLExecution(s"\n$sql\n$args", e)
      }
    def mutate(q: (String, List[Any])): Int = mutate(q._1, q._2)
    /**
      * Interpolates arguments into a prepared statement.
      */
    private def prepareStatement(sql: String, args: Iterable[Any]): PreparedStatement = {
      val stmt = connection prepareStatement sql
      args.view.zip(Stream from 1) foreach {
        case (value, index) =>
          value match {
            case x: String => stmt.setString(index, x)
            case x: Int => stmt.setInt(index, x)
            case x: Long => stmt.setLong(index, x)
            case x: Boolean => stmt.setBoolean(index, x)
            case x: Double => stmt.setDouble(index, x)
            case x: Float => stmt.setFloat(index, x)
            case x: Byte => stmt.setByte(index, x)
            case x: java.sql.Date => stmt.setDate(index, x)
            case x: java.sql.Time => stmt.setTime(index, x)
            case x: java.sql.Timestamp => stmt.setTimestamp(index, x)
            case x: java.util.Date =>
              stmt.setTimestamp(index, new Timestamp(x.getTime))
              // http://crafted-software.blogspot.com/2013/03/uuid-values-from-jdbc-to-postgres.html
            case x: java.util.UUID => stmt.setObject(index, x)
            case null =>
              sys.error(s"Null values not allowed: use Null wrapper or a literal NULL.")
            case n@Null() =>
              val cn = n.t.runtimeClass.getCanonicalName
              stmt.setNull(index,
                if (cn == classOf[Int].getCanonicalName)
                  java.sql.JDBCType.INTEGER.getVendorTypeNumber
                else if (cn == classOf[Long].getCanonicalName)
                  java.sql.JDBCType.BIGINT.getVendorTypeNumber
                else if (cn == classOf[Double].getCanonicalName)
                  java.sql.JDBCType.DOUBLE.getVendorTypeNumber
                else if (cn == classOf[Float].getCanonicalName)
                  java.sql.JDBCType.FLOAT.getVendorTypeNumber
                // This gets weird because there are a lot of string types.
                // This seems to work, for now.  May need wrapper class to distinguish them, in the future.
                else if (cn == classOf[String].getCanonicalName)
                  java.sql.JDBCType.VARCHAR.getVendorTypeNumber
                else sys error s"Unhandled NULL data type: $cn"
              )
            case _ =>
              val className = try value.getClass.getCanonicalName catch {
                case _: Throwable => value.getClass.toString
              }
              sys.error(
                s"Unhandled data type in prepared statement: ${value.toString} [$className}]"
              )
          }
      }
      stmt
    }
  }
}
