package wafna.rdb4s
import java.sql.{ResultSet, SQLSyntaxErrorException}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.TimeoutException
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
package object db {
  type JDBCConnection = java.sql.Connection
  /**
    * Deriving this on an object ensures the driver class gets loaded.
    * Put the other collateral (the connection type and the connection manager) in here.
    */
  abstract class Database[D <: java.sql.Driver](implicit m: ClassTag[D]) {
    final val driverName: String = m.runtimeClass.getCanonicalName
  }
  /**
    * Points to where the promise is awaited along with the root cause.
    */
  class CPReflectedException(e: Throwable)
      extends Exception(e)
  class CPTimeoutException(timeout: FiniteDuration)
      extends TimeoutException(s"Timeout exceeded: $timeout")
  class CPTaskRejectedException(msg: String) extends Exception(msg)
  /**
    * Results deferred to future execution in the connection pool.
    */
  abstract class DBPromise[T] {
    self: DBPromise[T] =>
    def take(timeOut: FiniteDuration): Try[T]
    def reflect(timeOut: FiniteDuration): T =
      take(timeOut).transform(Success(_), e => Failure(new CPReflectedException(e))).get
    def map[R](f: T => R): DBPromise[R] =
      (timeOut: FiniteDuration) => self.take(timeOut).map(f(_))
  }
  object DBPromise {
    def apply[T](t: T): DBPromise[T] = _ => Success(t)
  }
  /**
    * Produces vendor specific connections.
    */
  abstract class ConnectionManagerRDB4S[R <: ConnectionRDB4S] {
    def createConnection(): R
  }
  private object ConnectionRDB4S {
    /**
      * Iterate a ResultSet.
      */
    class RSIterator(rs: ResultSet) extends Iterator[RSCursor]() {
      override def hasNext: Boolean = rs.next()
      override def next(): RSCursor = new RSCursor(rs)
    }
  }
  /**
    * Vendor specific wrapper for a JDBC connection.
    */
  abstract class ConnectionRDB4S(protected[rdb4s] val connection: JDBCConnection)
      extends AutoCloseable {
    import java.sql.{PreparedStatement, Timestamp}
    import ConnectionRDB4S._
    override protected[rdb4s] def close(): Unit = connection.close()
    /**
      * @see java.sql.PreparedStatement.executeQuery
      */
    def query[T](sql: String, args: List[Any] = Nil)(extraction: Extraction[T]): List[T] =
      try bracket(prepareStatement(sql, args))(_.close) { stmt =>
        bracket({
          try stmt.executeQuery()
          catch {
            case e: SQLSyntaxErrorException =>
              sys error s"${e.getMessage}\n-- SQL\n$sql\n--"
          }
        })(Option(_).foreach(_.close()))(r => new RSIterator(r).map(extraction).toList)
      } catch {
        case e: java.sql.SQLException => throw new RuntimeException(sql, e)
      }
    def query[T](q: (String, List[Any])): Extraction[T] => List[T] = query(q._1, q._2)
    /**
      * @see java.sql.PreparedStatement.executeUpdate
      */
    def mutate(sql: String, args: List[Any] = Nil): Int =
      try bracket(prepareStatement(sql, args))(_.close())(_.executeUpdate())
      catch {
        case e: java.sql.SQLException => throw new RuntimeException(sql, e)
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
            case null =>
              sys.error(s"Null values not allowed: use Null wrapper.")
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
              sys.error(
                s"Unhandled data type in prepared statement: ${value.toString} [${value.getClass.getCanonicalName}}]"
              )
          }
      }
      stmt
    }
  }
  /**
    * Pulls values in sequence from ResultSet, obviating the need to remember indices.
    */
  class RSCursor(val rs: ResultSet) {
    private var nth = 0
    @inline private def next[T](f: Int => T): T = {
      nth += 1
      f(nth)
    }
    @inline private def maybe[T](f: Int => T): Option[T] = {
      val v = next(f)
      if (rs.wasNull) None else Some(v)
    }
    abstract class Type[T](f: Int => T) {
      def get: T = next(f)
      def opt: Option[T] = maybe(f)
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
  // Guards a resource.
  private def bracket[R, T](resource: R)(dispose: R => Unit)(consume: R => T): T =
    try consume(resource)
    finally dispose(resource)
  /**
    * This forces clients to declare explicitly how they want transactions to be managed.
    */
  class TransactionManager[R <: ConnectionRDB4S](connection: R) {
    // This ensures autoCommit is left the way it started, which is probably over kill.
    @inline private def bracketAutoCommit[T](use: JDBCConnection => T): T = {
      val cx: JDBCConnection = connection.connection
      bracket(cx.getAutoCommit)(cx.setAutoCommit) { _ => use(cx)
      }
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
  /**
    * Listens to events in the connection pool.  Used for gathering metrics.
    * These methods are called in line rather than being driven off a message bus
    * so make it quick.
    */
  class ConnectionPoolListener {
    def poolStart(): Unit = Unit
    def poolStop(queueSize: Int): Unit = Unit
    def taskStart(queueSize: Int, timeInQueue: Duration): Unit = Unit
    def taskStop(queueSize: Int, timeToExecute: Duration): Unit = Unit
    def threadStart(threadPoolSize: Int): Unit = Unit
    def threadStop(threadPoolSize: Int): Unit = Unit
  }
  /**
    * Static instance for implicit defaults.
    */
  object ConnectionPoolListener extends ConnectionPoolListener
  /**
    * The client interface.
    */
  trait ConnectionPool[R <: ConnectionRDB4S] {
    def addTask[T](task: TransactionManager[R] => T): DBPromise[T]
    def autoCommit[T](task: R => T): DBPromise[T] = addTask(_ autoCommit task)
    def blockCommit[T](task: R => T): DBPromise[T] = addTask(_ blockCommit task)
  }
  /**
    * A dynamically sized thread pool where each thread is assigned one connection.
    */
  object ConnectionPool {
    class Config() {
      private var _name: Option[String] = None
      private var _maxPoolSize: Option[Int] = None
      private var _minPoolSize: Option[Int] = None
      private var _idleTimeout: Option[FiniteDuration] = None
      private var _maxQueueSize: Option[Int] = None
      def name: Option[String] = _name
      def name(n: String): this.type = {
        _name = Some(n)
        this
      }
      def maxPoolSize: Option[Int] = _maxPoolSize
      def maxPoolSize(i: Int): this.type = {
        if (1 > i) throw new IllegalArgumentException(s"maxPoolSize must be positive, got $i")
        _maxPoolSize = Some(i)
        this
      }
      def minPoolSize: Option[Int] = _minPoolSize
      def minPoolSize(i: Int): this.type = {
        if (0 > i) throw new IllegalArgumentException(s"minPoolSize must be non-negative, got $i")
        _minPoolSize = Some(i)
        this
      }
      def idleTimeout: Option[FiniteDuration] = _idleTimeout
      def idleTimeout(t: FiniteDuration): this.type = {
        if (1 > t.toMillis) throw new IllegalArgumentException(s"idleTimeout must be positive, got $t")
        _idleTimeout = Some(t)
        this
      }
      def maxQueueSize: Option[Int] = _maxQueueSize
      def maxQueueSize(i: Int): this.type = {
        if (1 > i) throw new IllegalArgumentException(s"maxQueueSize must be positive, got $i")
        _maxQueueSize = Some(i)
        this
      }
    }
    def apply[R <: ConnectionRDB4S](config: ConnectionPool.Config, connectionManager: ConnectionManagerRDB4S[R])(
        borrow: ConnectionPool[R] => Unit)(implicit listener: ConnectionPoolListener = ConnectionPoolListener): Unit = {
      import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
      val poolName = config.name.getOrElse(sys error "name required.")
      val maxPoolSize = config.maxPoolSize.getOrElse(sys error "maxPoolSize required.")
      val minPoolSize = config.minPoolSize.getOrElse(0)
      if (minPoolSize > maxPoolSize) sys error s"minPoolSize ($minPoolSize) > maxPoolSize ($maxPoolSize)"
      val idleTimeout = config.idleTimeout.getOrElse(sys error "idleTimeout required.")
      val maxQueueSize = config.maxQueueSize.getOrElse(sys error "maxQueueSize required.")
      val active = new AtomicBoolean(true)
      val synch = new Object()
      val queue = new LinkedBlockingQueue[Task[_]]()
      var threadPool = Map[String, Thread]()
      def createThread(): Unit = {
        val runnerName: String = s"$poolName-${threadPool.size}"
        val thread = new Thread(new TaskRunner(runnerName), runnerName)
        threadPool += (runnerName -> thread)
        thread.start()
      }
      def adjustThreadPool(): Unit = if (active.get) synch synchronized {
        val poolSize = threadPool.size
        if (poolSize < minPoolSize || (poolSize < maxPoolSize && !queue.isEmpty)) {
          createThread()
        }
      }
      class TaskResult[T]() extends DBPromise[T] {
        private var result: Option[Try[T]] = None
        def put(v: Try[T]): Unit = synchronized {
          if (result.isDefined)
            sys error s"Cannot put to fulfilled result."
          result = Some(v)
          notifyAll()
        }
        def take(timeOut: FiniteDuration): Try[T] = synchronized {
          var remaining = timeOut.toMillis
          while (result.isEmpty && 0 < remaining) {
            val t0 = System.currentTimeMillis()
            wait(remaining)
            remaining -= (System.currentTimeMillis() - t0)
          }
          result.getOrElse(throw new CPTimeoutException(timeOut))
        }
      }
      class Task[T](val result: TaskResult[T], task: TransactionManager[R] => T) {
        val created: Long = System.currentTimeMillis()
        def run(resource: TransactionManager[R]): Unit = result put Try(task(resource))
      }
      class TaskRunner[T](val runnerName: String) extends Runnable {
        override def run(): Unit = {
          listener threadStart threadPool.size
          try {
            bracket(connectionManager.createConnection())(_.close()) { connection =>
              @tailrec def runTasks(): Unit =
                Option(
                  queue.poll(idleTimeout.toMillis, TimeUnit.MILLISECONDS)
                ) match {
                  case None =>
                    // nothing on the queue: time to bail
                    Unit
                  case Some(task) =>
                    listener.taskStart(queue.size(), (System.currentTimeMillis() - task.created).millis)
                    val t0 = System.currentTimeMillis()
                    try
                      task.run(new TransactionManager(connection))
                    finally
                      listener.taskStop(queue.size(), (System.currentTimeMillis() - t0).millis)
                    runTasks()
                }
              runTasks()
            }
          } catch {
            case _: InterruptedException =>
            case e: Throwable =>
              throw new RuntimeException(s"ConnectionPool: exception in resource block: $runnerName", e)
          }
          finally {
            synch synchronized {
              threadPool = threadPool - Thread.currentThread().getName
            }
            listener.threadStop(threadPool.size)
            // This thread is biffed but there still may be more to do.
            // Otherwise we'd have to wait to be prompted by the addition of another task.
            adjustThreadPool()
          }
        }
      }
      try {
        listener.poolStart()
        // The initial thread pool (reified by toArray).
        Iterator.continually(createThread()).take(minPoolSize).toArray
        borrow(new ConnectionPool[R] {
          override def addTask[T](task: TransactionManager[R] => T): DBPromise[T] = if (!active.get)
            throw new CPTaskRejectedException(s"ConnectionPool is shutting down: no tasks will be accepted.")
          else {
            val result = new TaskResult[T]()
            // important to add the task before deciding whether to add a thread.
            if (maxQueueSize > queue.size()) {
              queue.put(new Task[T](result, task))
              adjustThreadPool()
              result
            } else {
              throw new CPTaskRejectedException(s"max queue size exceeded: $maxQueueSize")
            }
          }
        })
      } finally {
        active set false
        val queueSize = queue.size()
        synch synchronized {
          queue.clear()
        }
        listener.poolStop(queueSize)
        threadPool.foreach(_._2.interrupt())
        threadPool.foreach(_._2.join())
      }
    }
  }
}
