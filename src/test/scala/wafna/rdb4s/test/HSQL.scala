package wafna.rdb4s.test
import wafna.rdb4s.db._
import scala.concurrent.duration.Duration
/**
  * An example of a Database adaptor.
  */
object HSQL extends Database[org.hsqldb.jdbcDriver] {
  /**
    * A database is a pool of vendor specific connections.
    */
  type DB = ConnectionPool[Connection]
  /**
    * In here we put in any vendor specific functionality we want.
    */
  class Connection(connection: JDBCConnection) extends ConnectionRDB4S(connection) {
    def lastInsertId(): Int = query("CALL IDENTITY()", Nil)(_.int.get).head
  }
  /**
    * Construct this with whatever information you need to make a connection.
    * Parameterize on our connection type.
    */
  class ConnectionManager(database: String) extends ConnectionManagerRDB4S[Connection] {
    override def createConnection(): HSQL.Connection =
      new Connection(java.sql.DriverManager.getConnection(s"jdbc:hsqldb:mem:$database"))
  }
  /**
    * One liner convenience method.
    */
  def apply(database: String, config: ConnectionPool.Config)(borrow: DB => Unit)(
      implicit listener: ConnectionPoolListener): Unit =
    ConnectionPool[Connection](config, new ConnectionManager(database))(borrow)
}
