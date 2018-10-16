package wafna.rdb4s.test

import java.util.concurrent.atomic.AtomicBoolean

import com.codahale.metrics.{Meter, MetricRegistry}
import wafna.rdb4s.db.ConnectionPoolListener

import scala.concurrent.duration.Duration
object ConnectionPoolEventCounter {
  // For testing en masse.
  case class ConnectionPoolEventCounts(poolStart: Boolean, poolStop: Boolean, taskStart: Long, taskStop: Long, threadStart: Long, threadStop: Long)
  case class MeterSnapshot(count: Long, rateMean: Double, rate1m: Double, rate5m: Double, rate15m: Double)
  object MeterSnapshot {
    def apply(meter: Meter): MeterSnapshot = MeterSnapshot(meter.getCount, meter.getMeanRate, meter.getOneMinuteRate, meter.getFiveMinuteRate, meter.getFifteenMinuteRate)
  }
  case class PoolSnapshot(taskStart: MeterSnapshot, tasksStop: MeterSnapshot, threadStart: MeterSnapshot, threadStop: MeterSnapshot)
}
/**
  * Counts each event.  Useful for testing behavior.
  */
class ConnectionPoolEventCounter extends ConnectionPoolListener {
  val metrics: MetricRegistry = new MetricRegistry()
  import ConnectionPoolEventCounter._
  val _poolStart: AtomicBoolean = new AtomicBoolean(false)
  val _poolStop: AtomicBoolean = new AtomicBoolean(false)
  val _taskStart: Meter = metrics meter "taskStart"
  val _taskStop: Meter = metrics meter "taskStop"
  val _threadStart: Meter = metrics meter "threadStart"
  val _threadStop: Meter = metrics meter "threadStop"
  override def poolStart(): Unit = if (_poolStart.get()) sys error "poolStart" else _poolStart set true
  override def poolStop(queueSize: Int): Unit = if (_poolStop.get()) sys error "poolStop" else _poolStop set true
  override def taskStart(queueSize: Int, timeInQueue: Duration): Unit = _taskStart.mark()
  override def taskStop(queueSize: Int, timeToExecute: Duration): Unit = _taskStop.mark()
  override def threadStart(threadPoolSize: Int): Unit = _threadStart.mark()
  override def threadStop(threadPoolSize: Int): Unit = _threadStop.mark()
  def getCounts: ConnectionPoolEventCounts =
    ConnectionPoolEventCounts(_poolStart.get(), _poolStop.get(),
      _threadStart.getCount, _threadStop.getCount, _taskStart.getCount, _taskStop.getCount)
  def getMetrics = ConnectionPoolEventCounter.PoolSnapshot(MeterSnapshot(_threadStart),MeterSnapshot(_threadStop),MeterSnapshot(_taskStart),MeterSnapshot(_taskStop))
}
