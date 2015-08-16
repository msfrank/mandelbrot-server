package io.mandelbrot.core.state

import akka.actor._
import com.typesafe.config.Config
import org.joda.time.{DateTimeZone, DateTime}
import scala.collection.JavaConversions._
import java.util

import io.mandelbrot.core.model._
import io.mandelbrot.core.{ResourceNotFound, ApiException}

class TestStatePersister(settings: TestStatePersisterSettings) extends Actor with ActorLogging {

  val checkStatus = new util.HashMap[CheckRefGeneration,util.TreeMap[DateTime,UpdateStatus]]()

  def receive = {

    case op: GetStatus =>
      val checkRefGeneration = CheckRefGeneration(op.checkRef, op.generation)
      checkStatus.get(checkRefGeneration) match {
        case null =>
          sender() ! GetStatusResult(op, None)
        case history if history.lastEntry() == null =>
          sender() ! GetStatusResult(op, None)
        case history =>
          val lastEntry = history.lastEntry()
          val status = Some(lastEntry.getValue.status)
          sender() ! GetStatusResult(op, status)
      }

    case op: UpdateStatus =>
      val checkRefGeneration = CheckRefGeneration(op.checkRef, op.status.generation)
      checkStatus.get(checkRefGeneration) match {
        case null =>
          val history = new util.TreeMap[DateTime,UpdateStatus]()
          history.put(op.status.timestamp, op)
          checkStatus.put(checkRefGeneration, history)
          sender() ! UpdateStatusResult(op)
        case history =>
          history.put(op.status.timestamp, op)
          sender() ! UpdateStatusResult(op)
      }

    case op: DeleteStatus =>
      val checkRefGeneration = CheckRefGeneration(op.checkRef, op.generation)
      checkStatus.remove(checkRefGeneration)
      sender() ! DeleteStatusResult(op)

    /* return condition history */
    case op: GetStatusHistory =>
      try {
        val history = getHistory(op.checkRef, op.generation, op.from, op.to, op.last).map(_.status)
        val page = if (history.length > op.limit) {
          val subset = history.take(op.limit)
          val last = subset.lastOption.map(_.timestamp.getMillis.toString)
          CheckStatusPage(subset, last, exhausted = false)
        } else {
          CheckStatusPage(history, last = None, exhausted = true)
        }
        sender() ! GetStatusHistoryResult(op, page)
      } catch {
        case ex: Throwable => sender() ! StateServiceOperationFailed(op, ex)
      }

    /* return condition history */
    case op: GetConditionHistory =>
      try {
        val history = getHistory(op.checkRef, op.generation, op.from, op.to, op.last).map(status2condition)
        val page = if (history.length > op.limit) {
          val subset = history.take(op.limit)
          val last = subset.lastOption.map(_.timestamp.getMillis.toString)
          CheckConditionPage(subset, last, exhausted = false)
        } else {
          CheckConditionPage(history, last = None, exhausted = true)
        }
        sender() ! GetConditionHistoryResult(op, page)
      } catch {
        case ex: Throwable => sender() ! StateServiceOperationFailed(op, ex)
      }

    /* return notifications history */
    case op: GetNotificationsHistory =>
      try {
        val history = getHistory(op.checkRef, op.generation, op.from, op.to, op.last).map(status2notifications)
        val page = if (history.length > op.limit) {
          val subset = history.take(op.limit)
          val last = subset.lastOption.map(_.timestamp.getMillis.toString)
          CheckNotificationsPage(subset, last, exhausted = false)
        } else {
          CheckNotificationsPage(history, last = None, exhausted = true)
        }
        sender() ! GetNotificationsHistoryResult(op, page)
      } catch {
        case ex: Throwable => sender() ! StateServiceOperationFailed(op, ex)
      }

    /* return metrics history */
    case op: GetMetricsHistory =>
      try {
        val history = getHistory(op.checkRef, op.generation, op.from, op.to, op.last).map(status2metrics)
        val page = if (history.length > op.limit) {
          val subset = history.take(op.limit)
          val last = subset.lastOption.map(_.timestamp.getMillis.toString)
          CheckMetricsPage(subset, last, exhausted = false)
        } else {
          CheckMetricsPage(history, last = None, exhausted = true)
        }
        sender() ! GetMetricsHistoryResult(op, page)
      } catch {
        case ex: Throwable => sender() ! StateServiceOperationFailed(op, ex)
      }
  }

  /**
   * return the history entries matching the specified parameters.
   */
  def getHistory(checkRef: CheckRef,
                 generation: Long,
                 from: Option[DateTime],
                 to: Option[DateTime],
                 last: Option[String]): Vector[UpdateStatus] = {
    val checkRefGeneration = CheckRefGeneration(checkRef, generation)
    checkStatus.get(checkRefGeneration) match {
      // if there is no check, then raise ResourceNotFound
      case null =>
        throw ApiException(ResourceNotFound)
      // if no timeseries params are specified, return the last entry
      case history if from.isEmpty && to.isEmpty =>
        history.lastEntry() match {
          case null => Vector.empty
          case entry => Vector(entry.getValue)
        }
      // otherwise return the subset of entries
      case history =>
        val _last: Option[DateTime] = last.map(s => new DateTime(s.toLong).withZone(DateTimeZone.UTC))
        val _from: DateTime = _last.getOrElse(from.getOrElse(new DateTime(0, DateTimeZone.UTC)))
        val _to: DateTime = to.getOrElse(new DateTime(Long.MaxValue, DateTimeZone.UTC))
        history.subMap(_from, false, _to, true).map(entry => entry._2).toVector
    }
  }

  /**
   * convert the specified raw status into a Condition.
   */
  def status2condition(updateCheckStatus: UpdateStatus): CheckCondition = {
    val status = updateCheckStatus.status
    CheckCondition(status.generation, status.timestamp, status.lifecycle, status.summary,
      status.health, status.correlation, status.acknowledged, status.squelched)
  }

  /**
   * convert the specified raw status into a Notification set.
   */
  def status2notifications(updateCheckStatus: UpdateStatus): CheckNotifications = {
    CheckNotifications(updateCheckStatus.status.generation,
      updateCheckStatus.status.timestamp, updateCheckStatus.notifications)
  }

  /**
   * convert the specified raw status into a Metrics set.
   */
  def status2metrics(updateCheckStatus: UpdateStatus): CheckMetrics = {
    CheckMetrics(updateCheckStatus.status.generation,
      updateCheckStatus.status.timestamp, updateCheckStatus.status.metrics)
  }
}

object TestStatePersister {
  def props(settings: TestStatePersisterSettings) = Props(classOf[TestStatePersister], settings)
}

case class CheckRefGeneration(checkRef: CheckRef, generation: Long) extends Ordered[CheckRefGeneration] {
  import scala.math.Ordered.orderingToOrdered
  def compare(other: CheckRefGeneration): Int = {
    (checkRef.toString, generation).compare((other.checkRef.toString, other.generation))
  }
}

case class TestStatePersisterSettings()

class TestStatePersisterExtension extends StatePersisterExtension {
  type Settings = TestStatePersisterSettings
  def configure(config: Config): Settings = TestStatePersisterSettings()
  def props(settings: Settings): Props = TestStatePersister.props(settings)
}

