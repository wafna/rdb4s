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
  def apply[R <: Connection](config: ConnectionPool.Config, connectionManager: ConnectionManager[R])(
      borrow: ConnectionPool[R] => Unit)(implicit listener: ConnectionPoolListener = ConnectionPoolListener): Unit = {
    import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
    val poolName = config.name getOrElse sys.error("name required.")
    if (poolName.isEmpty) sys error s"poolName must be non-empty."
    val maxPoolSize = config.maxPoolSize getOrElse sys.error("maxPoolSize required.")
    if (1 > maxPoolSize) sys error s"maxPoolSize must be positive"
    val minPoolSize = config.minPoolSize getOrElse 0
    if (minPoolSize > maxPoolSize) sys error s"minPoolSize ($minPoolSize) > maxPoolSize ($maxPoolSize)"
    val idleTimeout = config.idleTimeout getOrElse sys.error("idleTimeout required.")
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
