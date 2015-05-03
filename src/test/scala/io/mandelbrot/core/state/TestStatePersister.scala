package io.mandelbrot.core.state

import akka.actor._
import com.typesafe.config.Config
import io.mandelbrot.core.{ResourceNotFound, ApiException, NotImplemented}
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTimeZone, DateTime}
import scala.collection.JavaConversions._
import java.util

import io.mandelbrot.core.model._

class TestStatePersister(settings: TestStatePersisterSettings) extends Actor with ActorLogging {

  val state = new util.HashMap[CheckRef,util.TreeMap[DateTime,UpdateCheckStatus]]()
  val datetimeParser = ISODateTimeFormat.dateTimeParser().withZoneUTC()

  def receive = {

    case op: InitializeCheckStatus =>
      state.get(op.checkRef) match {
        case null =>
          sender() ! InitializeCheckStatusResult(op, None)
        case history if history.lastEntry() == null =>
          sender() ! InitializeCheckStatusResult(op, None)
        case history =>
          val lastEntry = history.lastEntry()
          val status = Some(lastEntry.getValue.status)
          sender() ! InitializeCheckStatusResult(op, status)
      }

    case op: UpdateCheckStatus =>
      state.get(op.checkRef) match {
        case null =>
          val history = new util.TreeMap[DateTime,UpdateCheckStatus]()
          history.put(op.status.timestamp, op)
          state.put(op.checkRef, history)
          sender() ! UpdateCheckStatusResult(op)
        case history =>
          history.put(op.status.timestamp, op)
          sender() ! UpdateCheckStatusResult(op)
      }

    case op: DeleteCheckStatus =>
      state.remove(op.checkRef)
      sender() ! DeleteCheckStatusResult(op)

    case op: TrimCheckHistory =>
      state.get(op.checkRef) match {
        case null =>
          sender() ! TrimCheckHistoryResult(op)
        case history =>
          history.headMap(op.until).map(_._1).foreach(history.remove)
          sender() ! TrimCheckHistoryResult(op)
      }

    case op: GetConditionHistory =>
      state.get(op.checkRef) match {
        case null =>
          sender() ! StateServiceOperationFailed(op, ApiException(ResourceNotFound))
        case history if op.from.isEmpty && op.to.isEmpty =>
          history.lastEntry() match {
            case null =>
              sender() ! GetConditionHistoryResult(op, CheckConditionPage(Vector.empty, None, exhausted = true))
            case entry =>
              val status = Vector(status2condition(entry.getValue))
              sender() ! GetConditionHistoryResult(op, CheckConditionPage(status, None, exhausted = true))
          }
        case history =>
          val last: Option[DateTime] = op.last.map(datetimeParser.parseDateTime)
          val from: DateTime = last.getOrElse(op.from.getOrElse(new DateTime(0, DateTimeZone.UTC)))
          val to: DateTime = op.to.getOrElse(new DateTime(Long.MaxValue, DateTimeZone.UTC))
          val conditions = history.subMap(from, false, to, false)
            .map(entry => status2condition(entry._2)).toVector
          val page = if (conditions.length > op.limit) {
            val subset = conditions.take(op.limit)
            val last = subset.lastOption.map(_.timestamp.getMillis.toString)
            val exhausted = false
            CheckConditionPage(subset, last, exhausted)
          } else {
            CheckConditionPage(conditions, last = None, exhausted = true)
          }
          sender() ! GetConditionHistoryResult(op, page)
      }

    case op: GetNotificationHistory =>
      sender() ! StateServiceOperationFailed(op, NotImplemented)

    case op: GetMetricHistory =>
      sender() ! StateServiceOperationFailed(op, NotImplemented)
  }

  def status2condition(updateCheckStatus: UpdateCheckStatus): CheckCondition = {
    val status = updateCheckStatus.status
    CheckCondition(status.timestamp, status.lifecycle, status.summary, status.health,
      status.correlation, status.acknowledged, status.squelched)
  }
}

object TestStatePersister {
  def props(settings: TestStatePersisterSettings) = Props(classOf[TestStatePersister], settings)
}

case class TestStatePersisterSettings()

class TestStatePersisterExtension extends StatePersisterExtension {
  type Settings = TestStatePersisterSettings
  def configure(config: Config): Settings = TestStatePersisterSettings()
  def props(settings: Settings): Props = TestStatePersister.props(settings)
}

