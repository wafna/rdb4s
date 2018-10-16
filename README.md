# RDB4S

A complete relational database solution for Scala.

* Light weight, lock free, dynamic connection pooling.
* Fine grained control over transactional boundaries.
* Easy support for vendor specific functionality.
* Always safe handling of parameters.
* Independence of projections and tables.
* Optional DSL for SQL for reusable projections.

The goals of the project are to provide a close to the metal framework around JDBC 
in a convenient Scala API, 
a fast and lightweight concurrent execution framework for database calls, 
and a convenient method of composing and reusing bits of SQL.

## Connection Pool

Instead of keeping a pool of connections to be accessed by a pool of threads this project
simply identifies the two.  Each thread has a dedicated connection.  This avoids resource 
locking or any mismatch between the number of connections maintained and the number of 
connections that are actually in use.  Instead of testing connections for staleness
we simply discard them.

Here is an example of defining a database.

<pre>
import wafna.rdb4s.cp._
import scala.concurrent.duration.Duration
/**
  * Adaptor for the HSQL in-memory database.
  */
object HSQL extends Database[org.hsqldb.jdbcDriver] {
  /**
    * A database is a pool of vendor specific connections.
    * This type alias is useful for clients.
    */
  type DB = ConnectionPool[Connection]
  /**
    * In here we put in any vendor specific functionality we want.
    */
  class Connection(connection: JDBCConnection) extends ConnectionRDB4S(connection) {
    def lastInsertId(): Int = query("CALL IDENTITY()", Nil)(_.getInt).head
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
  def apply(database: String, poolName: String, maxSize: Int, idleTimeout: Duration)(borrow: DB => Unit)(
      implicit listener: ConnectionPoolListener = ConnectionPoolListenerNOOP): Unit =
    ConnectionPool[Connection](poolName, maxSize, idleTimeout, new ConnectionManager(database))(borrow)
}
</pre>

There's very little work to be done. All you need to do is figure out how to make 
a JDBC connection and decide what vendor specific functionality to put in.

Here is an example of using it.

<pre>
import scala.concurrent.duration._
HSQL("database", "thread-pool", 3, 1.second) { db =>
  val timeout = 1.second
  // pick a commit strategy
  db autoCommit { cx =>
    cx.query(
      // The SQL plus the values to interpolate into the prepared statement (in lexical order).
      "select id, name from users where id = ?", List(42))(
      // Marshall the result to a record.
      // Retrieve the fields in the order they were selected.
      r => (r.getInt, r.getString)).headOption
    // Wait for the result.
  } reflect timeout match {
    case None =>
      println("No user found.")
    case Some(user) =>
      println(user)
  }
}
</pre>

The results of operations on connections are DBPromises.  These are like futures in that 
they are placeholders for values yet to be calculated.  You can wait for their values in either 
of two ways.  By calling `take` you wait until you get a `Try` of the value.  By calling `reflect`
you also wait and then get either the value or a thrown exception.  This exception will be 
the exception thrown in the task wrapped in an exception pointing to the place where `reflect` was called.

## DSL for SQL

The DSL provides a fluent API for constructing SQL that closely matches SQL syntax. 
Each construct produces an immutable object suitable for reuse in other compositions.

Each table you reference needs to be modeled, like so:

<pre>
class TUser(alias: String) extends Table("user", alias) {
    val id: TField = field("id")
    val name: TField = field("name")
}
</pre>

The fields can then be referenced directly in SQL constructs.  Before we do that we need to
alias them, however.

<pre>
val u = new TUser("u")
</pre>

These are the aliases that we will reference in our SQL statements, like so:

<pre>
select(u.id, u.name).from(u).where(u.name like "%abc%")
</pre>

Literal values, such as that on the right hand side of the like predicate, are replaced by
query parameters and collected for interpolation into the prepared statement.

These DSL expressions can be passed directly to the `query` and `mutate` methods on `ConnectionRDB4S`, 
which, of course, is the base class for your vendor specific derived connection type. 

## Using in a Project

The `TestDB` class gives a good example of how to organize and implement an 
API around a database.

## Metrics

The `ConnectionPoolListener` trait is provided for tracking events in the connection pool. 
A NOOP version is provided making its use non-mandatory.  The `test` src contains an example 
that drives Codahale metrics.