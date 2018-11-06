package wafna.rdb4s.db
import java.sql.ResultSet
/**
  * Pulls values in sequence from ResultSet, obviating the need to remember indices.
  */
class RowCursor(val rs: ResultSet) {
  private var nth = 0
  @inline final private def next[T](f: Int => T): T = {
    nth += 1
    f(nth)
  }
  abstract class Type[T](f: Int => T) {
    def get: T = next(f)
    def opt: Option[T] = maybe(f)
  }
  @inline final private def maybe[T](f: Int => T): Option[T] = {
    val v = next(f)
    if (rs.wasNull) None else Some(v)
  }
  object int extends Type[Int](rs.getInt)
  object long extends Type[Long](rs.getLong)
  object float extends Type[Float](rs.getFloat)
  object double extends Type[Double](rs.getDouble)
  object string extends Type[String](rs.getString)
  object bool extends Type[Boolean](rs.getBoolean)
  object byte extends Type[Byte](rs.getByte)
  object date extends Type[java.sql.Date](rs.getDate)
  object timestamp extends Type[java.sql.Timestamp](rs.getTimestamp)
  def getDoubleArray: scala.Array[Double] = next { i =>
    rs.getArray(i).getArray.asInstanceOf[scala.Array[_]] map { d =>
      d.asInstanceOf[Double]
    }
  }
}
