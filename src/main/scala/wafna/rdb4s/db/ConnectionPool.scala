package wafna.rdb4s.db
import java.util.concurrent.atomic.AtomicBoolean
import wafna.rdb4s.bracket
import wafna.rdb4s.db.RDB.{Connection, ConnectionManager, DBPromise}
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.Try
/**
  * The client interface.
  */
trait ConnectionPool[+R <: Connection] {
  def addTask[T](task: TransactionManager[R] => T): DBPromise[T]
  def autoCommit[T](task: R => T): DBPromise[T] = addTask(_ autoCommit task)
  def blockCommit[T](task: R => T): DBPromise[T] = addTask(_ blockCommit task)
}
/**
  * A dynamically sized thread pool where each thread is assigned one connection.
  */
object ConnectionPool {
  /**
    * Listens to events in the connection pool.  Used for gathering metrics.
    * These methods are called in line rather than being driven off a message bus
    * so make it quick.
    */
  class Listener {
    def poolStart(): Unit = Unit
    def poolStop(queueSize: Int): Unit = Unit
    def unrecoverableException(e: Throwable): Unit = Unit
    def taskStart(queueSize: Int, timeInQueue: FiniteDuration): Unit = Unit
    def taskStop(queueSize: Int, timeToExecute: FiniteDuration): Unit = Unit
    def threadStart(threadPoolSize: Int): Unit = Unit
    def threadStop(threadPoolSize: Int): Unit = Unit
    def connectionDropped(duration: FiniteDuration): Unit = Unit
    def threadIdleTimeout(duration: FiniteDuration): Unit = Unit
  }
  /**
    * Static instance for implicit defaults.
    */
  object DefaultListener extends Listener
  /**
    * Configuration for a ConnectionPool.
    * Non-reentrant.
    */
  class Config() {
    private var _name: Option[String] = None
    private var _maxPoolSize: Option[Int] = None
    private var _minPoolSize: Option[Int] = None
    private var _idleTimeout: Option[FiniteDuration] = None
    private var _connectionTestCycleLength: Option[FiniteDuration] = None
    private var _connectionTestTimeout: Option[Int] = None
    private var _maxQueueSize: Option[Int] = None
    def name: Option[String] = _name
    /**
      * Prepended to the thread names in the connection pool for easy identification.
      */
    def name(n: String): this.type = {
      _name = Some(n)
      this
    }
    def maxPoolSize: Option[Int] = _maxPoolSize
    /**
      * The maximum number of tasks allowed to queue waiting for execution.
      */
    def maxPoolSize(i: Int): this.type = {
      if (1 > i) throw new IllegalArgumentException(s"maxPoolSize must be positive, got $i")
      _maxPoolSize = Some(i)
      this
    }
    def minPoolSize: Option[Int] = _minPoolSize
    /**
      * The minimum number of connections to maintain.
      */
    def minPoolSize(i: Int): this.type = {
      if (0 > i) throw new IllegalArgumentException(s"minPoolSize must be non-negative, got $i")
      _minPoolSize = Some(i)
      this
    }
    def idleTimeout: Option[FiniteDuration] = _idleTimeout
    /**
      * The amount of time a thread's connection is not used before the connection is discarded (along with the thread, of course).
      */
    def idleTimeout(t: FiniteDuration): this.type = {
      if (1 > t.toMillis) throw new IllegalArgumentException(s"idleTimeout must be positive, got $t")
      _idleTimeout = Some(t)
      this
    }
    def connectionTestCycleLength: Option[FiniteDuration] = _connectionTestCycleLength
    /**
      * The interval of time between tests of the connection for viability.  This is implemented with Connection.isValid.
      * Note that if it's longer than idleTimeout it will never get tested unless there is a non-zero minPoolSize.
      */
    def connectionTestCycleLength(t: FiniteDuration): this.type = {
      if (0 > t.toMillis) throw new IllegalArgumentException(s"connectionTestCycleLengthTimeout must be positive, got $t")
      _connectionTestCycleLength = Some(t)
      this
    }
    def connectionTestTimeout: Option[Int] = _connectionTestTimeout
    /**
      * The length of time (in seconds, per Connection.isValid) to spend waiting for tasks to wait to test the connection for viability.
      */
    def connectionTestTimeout(t: Int): this.type = {
      if (0 > t) throw new IllegalArgumentException(s"connectionTestTimeout must be non-negative, got $t")
      _connectionTestTimeout = Some(t)
      this
    }
    def maxQueueSize: Option[Int] = _maxQueueSize
    /**
      * The maximum number of tasks to buffer in a FIFO queue.
      */
    def maxQueueSize(i: Int): this.type = {
      if (1 > i) throw new IllegalArgumentException(s"maxQueueSize must be positive, got $i")
      _maxQueueSize = Some(i)
      this
    }
  }
  def apply[R <: Connection](config: ConnectionPool.Config, connectionManager: ConnectionManager[R])(
      borrow: ConnectionPool[R] => Unit)(implicit listener: ConnectionPool.Listener = ConnectionPool.DefaultListener): Unit = {
    import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
    val poolName = config.name getOrElse sys.error("name required.")
    if (poolName.isEmpty) sys error s"poolName must be non-empty."
    val maxPoolSize = config.maxPoolSize getOrElse sys.error("maxPoolSize required.")
    if (1 > maxPoolSize) sys error s"maxPoolSize must be positive"
    val minPoolSize = config.minPoolSize getOrElse 0
    if (minPoolSize > maxPoolSize) sys error s"minPoolSize ($minPoolSize) > maxPoolSize ($maxPoolSize)"
    val idleTimeout = config.idleTimeout getOrElse sys.error("idleTimeout required.")
    val connectionTestCycleLength = config.connectionTestCycleLength getOrElse sys.error("connectionTestCycleLength required.")
    val connectionTestTimeout = config.connectionTestTimeout getOrElse sys.error("connectionTestTimeout required.")
    val maxQueueSize = config.maxQueueSize getOrElse sys.error("maxQueueSize required.")
    if (1 > maxQueueSize) sys error s"maxQueueSize must be positive"
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
        result.getOrElse(throw new CPException.Timeout(timeOut))
      }
    }
    class Task[T](val result: TaskResult[T], task: TransactionManager[R] => T) {
      val created: Long = System.currentTimeMillis()
      def run(resource: TransactionManager[R]): Unit = result put Try(task(resource))
    }
    class TaskRunner[T](val runnerName: String) extends Runnable {
      override def run(): Unit = {
        val start = System.currentTimeMillis()
        var lastTask = System.currentTimeMillis()
        listener threadStart threadPool.size
        try {
          bracket(connectionManager.createConnection())(_.close()) { connection =>
            @tailrec def runTasks(): Unit =
              Option(
                // We awaken from waiting on the queue to test the connection.
                // This strategy allows us to reserve testing connections for when there is no other activity.
                queue.poll(connectionTestCycleLength.toMillis, TimeUnit.MILLISECONDS)
              ) match {
                case None =>
                  val now = System.currentTimeMillis()
                  if (!connection.isValid(connectionTestTimeout)) {
                    // The simplest thing to do here is simply drop the whole thread and start a new one, with a new
                    // connection, of course, as needed, below.
                    listener.connectionDropped((now - start).milli)
                  } else if (synch.synchronized(threadPool.size) > minPoolSize && (now - lastTask) >= idleTimeout.toMillis) {
                    // Just pruning superfluous connections.
                    listener.threadIdleTimeout((now - start).milli)
                  } else {
                    runTasks()
                  }
                case Some(task) =>
                  // We consume the connection uncritically here because we know it's been used or tested no more than
                  // connectionTestCycleLength milliseconds ago.
                  lastTask = System.currentTimeMillis()
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
            if (connectionManager recoverableException e)
              throw new RuntimeException(s"ConnectionPool $poolName: exception in resource block: $runnerName", e)
            else {
              active set false
              listener.unrecoverableException(e)
              throw new RuntimeException (s"ConnectionPool $poolName: UNRECOVERABLE exception in resource block: $runnerName", e)
            }
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
      Iterator.continually(createThread()).take(minPoolSize) foreach identity
      borrow(new ConnectionPool[R] {
        override def addTask[T](task: TransactionManager[R] => T): DBPromise[T] =
          if (!active.get) {
            // The only way this could happen is if you closed the connection pool (terrible idea) or contrived some
            // way to hold it after the borrower method returned (even worse idea).
            throw new CPException.TaskRejected(s"ConnectionPool is shutting down: no tasks will be accepted.")
          } else {
            val result = new TaskResult[T]()
            // important to add the task before deciding whether to add a thread.
            if (maxQueueSize <= queue.size()) {
              throw new CPException.TaskRejected(s"max queue size exceeded: $maxQueueSize")
            } else {
              queue.put(new Task[T](result, task))
              adjustThreadPool()
              result
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
