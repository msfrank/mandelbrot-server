package io.mandelbrot.persistence.cassandra

import akka.actor.{Props, ActorLogging, Actor}
import akka.pattern.pipe
import com.typesafe.config.Config
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.Future

import io.mandelbrot.core.state._
import io.mandelbrot.core.model._
import io.mandelbrot.core._

/**
 *
 */
class CassandraStatePersister(settings: CassandraStatePersisterSettings) extends Actor with ActorLogging {
  import context.dispatcher

  // state
  val session = Cassandra(context.system).getSession
  val committedIndexDAL = new CommittedIndexDAL(settings, session, context.dispatcher)
  val checkStatusDAL = new CheckStatusDAL(settings, session, context.dispatcher)

  def receive = {

    case op: InitializeCheckStatus =>
      committedIndexDAL.getCommittedIndex(op.checkRef).flatMap {
        case committedIndex =>
          val epoch = EpochUtils.timestamp2epoch(committedIndex.current)
          checkStatusDAL.getCheckStatus(op.checkRef, epoch, committedIndex.current).map {
            case status => InitializeCheckStatusResult(op, Some(status))
          }
      }.recover {
        case ex: ApiException if ex.failure == ResourceNotFound =>
          InitializeCheckStatusResult(op, None)
        case ex: Throwable =>
          log.error(ex, "failed to initialize check status")
          StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: UpdateCheckStatus =>
      val commit = op.lastTimestamp match {
        case Some(last) =>
          committedIndexDAL.updateCommittedIndex(op.checkRef, op.status.timestamp, last)
        case None =>
          committedIndexDAL.initializeCommittedIndex(op.checkRef, op.status.timestamp)
      }
      val epoch = EpochUtils.timestamp2epoch(op.status.timestamp)
      commit.flatMap[UpdateCheckStatusResult] { _ =>
        checkStatusDAL.updateCheckStatus(op.checkRef, epoch, op.status, op.notifications).map {
          _ => UpdateCheckStatusResult(op)
        }
      }.recover {
        case ex: Throwable =>
          log.error(ex, "failed to update check status")
          StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    /* retrieve condition history for the specified CheckRef */
    case op: GetConditionHistory =>
      getEpoch(op.checkRef, op.from, op.to, op.last).flatMap {
        case (epoch, committed) =>
          checkStatusDAL.getCheckConditionHistory(op.checkRef, epoch, op.from, op.to, op.limit).map {
            case history => (epoch, committed, history)
          }
      }.map {
        case (epoch, committed, history) =>
          history.lastOption.map(_.timestamp).map(timestamp2last) match {
            case Some(timestamp) =>
              if (history.length < op.limit && epoch == EpochUtils.timestamp2epoch(committed.current))
                GetConditionHistoryResult(op, CheckConditionPage(history, Some(timestamp), exhausted = true))
              else
                GetConditionHistoryResult(op, CheckConditionPage(history, Some(timestamp), exhausted = false))
            case None =>
              val last = Some(EpochUtils.epoch2timestamp(epoch)).map(timestamp2last)
              val exhausted = if (epoch == EpochUtils.timestamp2epoch(committed.current)) true else false
              GetConditionHistoryResult(op, CheckConditionPage(history, last, exhausted))
          }
      }.recover {
        case ex: Throwable =>
          log.error(ex, "failed to get condition history")
          StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    /* retrieve notification history for the specified CheckRef */
    case op: GetNotificationHistory =>
      getEpoch(op.checkRef, op.from, op.to, op.last).flatMap {
        case (epoch, committed) =>
          checkStatusDAL.getCheckNotificationsHistory(op.checkRef, epoch, op.from, op.to, op.limit).map {
            case history => (epoch, committed, history)
          }
      }.map {
        case (epoch, committed, history) =>
          history.lastOption.map(_.timestamp).map(timestamp2last) match {
            case Some(timestamp) =>
              if (history.length < op.limit && epoch == EpochUtils.timestamp2epoch(committed.current))
                GetNotificationHistoryResult(op, CheckNotificationsPage(history, Some(timestamp), exhausted = true))
              else
                GetNotificationHistoryResult(op, CheckNotificationsPage(history, Some(timestamp), exhausted = false))
            case None =>
              val last = Some(EpochUtils.epoch2timestamp(epoch)).map(timestamp2last)
              val exhausted = if (epoch == EpochUtils.timestamp2epoch(committed.current)) true else false
              GetNotificationHistoryResult(op, CheckNotificationsPage(history, last, exhausted))
          }
      }.recover {
        case ex: Throwable =>
          log.error(ex, "failed to get notification history")
          StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    /* retrieve metrics history for the specified CheckRef */
    case op: GetMetricHistory =>
      getEpoch(op.checkRef, op.from, op.to, op.last).flatMap {
        case (epoch, committed) =>
          checkStatusDAL.getCheckMetricsHistory(op.checkRef, epoch, op.from, op.to, op.limit).map {
            case history => (epoch, committed, history)
          }
      }.map {
        case (epoch, committed, history) =>
          history.lastOption.map(_.timestamp).map(timestamp2last) match {
            case Some(timestamp) =>
              if (history.length < op.limit && epoch == EpochUtils.timestamp2epoch(committed.current))
                GetMetricHistoryResult(op, CheckMetricsPage(history, Some(timestamp), exhausted = true))
              else
                GetMetricHistoryResult(op, CheckMetricsPage(history, Some(timestamp), exhausted = false))
            case None =>
              val last = Some(EpochUtils.epoch2timestamp(epoch)).map(timestamp2last)
              val exhausted = if (epoch == EpochUtils.timestamp2epoch(committed.current)) true else false
              GetMetricHistoryResult(op, CheckMetricsPage(history, last, exhausted))
          }
      }.recover {
        case ex: Throwable =>
          log.error(ex, "failed to get metric history")
          StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())
  }

  /**
   * convert the specified string containing seconds since the UNIX epoch
   * to a DateTime with UTC timezone.
   */
  def last2timestamp(last: String): DateTime = new DateTime(last.toLong).withZone(DateTimeZone.UTC)

  /**
   * convert the specified DateTime with UTC timezone to a string containing
   * seconds since the UNIX epoch.
   */
  def timestamp2last(timestamp: DateTime): String = timestamp.getMillis.toString

  /**
   * determine which epoch to read from, given the specified constraints.
   */
  def getEpoch(checkRef: CheckRef,
               from: Option[DateTime],
               to: Option[DateTime],
               last: Option[String]): Future[(Long,CommittedIndex)] = {
    committedIndexDAL.getCommittedIndex(checkRef).flatMap[(Long,CommittedIndex)] { committed =>
      last.map(last2timestamp) match {
        // if last was specified, then return the epoch derived from the last timestamp
        case Some(timestamp) =>
          val epoch = EpochUtils.timestamp2epoch(timestamp)
          checkStatusDAL.checkIfEpochExhausted(checkRef, epoch, timestamp).map {
            case true => (epoch, committed)
            case false => (EpochUtils.nextEpoch(epoch), committed)
          }
        case None =>
          from match {
            // if from was specified, then return the epoch derived from the from timestamp
            case Some(timestamp) =>
              Future.successful((EpochUtils.timestamp2epoch(timestamp), committed))
            // if from was not specified, then use the initial timestamp in the committed index
            case None =>
              Future.successful((EpochUtils.timestamp2epoch(committed.initial), committed))
          }
      }
    }
  }
}

object CassandraStatePersister {
  def props(settings: CassandraStatePersisterSettings) = Props(classOf[CassandraStatePersister], settings)
}

case class CassandraStatePersisterSettings()

class CassandraStatePersisterExtension extends StatePersisterExtension {
  type Settings = CassandraStatePersisterSettings
  def configure(config: Config): Settings = CassandraStatePersisterSettings()
  def props(settings: Settings): Props = CassandraStatePersister.props(settings)
}
