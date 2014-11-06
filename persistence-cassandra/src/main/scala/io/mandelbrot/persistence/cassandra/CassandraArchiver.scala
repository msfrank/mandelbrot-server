package io.mandelbrot.persistence.cassandra

import akka.actor.{Cancellable, Props, ActorLogging, Actor}
import com.typesafe.config.Config
import com.datastax.driver.core.{Row, BoundStatement}
import org.joda.time.DateTime
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import java.util.Date

import io.mandelbrot.core.history._
import io.mandelbrot.core.system._
import io.mandelbrot.core.notification._
import io.mandelbrot.persistence.cassandra.CassandraArchiver.CassandraArchiverSettings
import io.mandelbrot.core.{NotImplemented, ApiException}

/**
 *
 */
class CassandraArchiver(settings: CassandraArchiverSettings) extends Actor with ActorLogging with Archiver with ArchiverStatements {
  import CassandraArchiver._
  import context.dispatcher

  // config
  val defaultLimit = 100

  // state
  var currEpoch: Long = millis2epoch(System.currentTimeMillis())
  var prevEpoch: Long = currEpoch - EPOCH_TERM
  var nextEpoch: Long = currEpoch + EPOCH_TERM
  var checkEpoch: Option[Cancellable] = None

  val session = Cassandra(context.system).getSession
  val keyspaceName = Cassandra(context.system).keyspaceName
  val statusHistoryTableName = "history_s"
  val notificationHistoryTableName = "history_n"
  val metricHistoryTableName = "history_m"

  session.execute(createStatusHistoryTable)
  session.execute(createNotificationHistoryTable)

  override def preStart(): Unit = {
    checkEpoch = Some(context.system.scheduler.schedule(1.minute, 1.minute, self, CheckEpoch))
  }

  override def postStop(): Unit = checkEpoch.foreach(_.cancel())

  def receive = {

    /* recalculate the epoch if necessary */
    case CheckEpoch =>
      val epoch = System.currentTimeMillis()
      if (epoch >= nextEpoch) {
        prevEpoch = currEpoch
        currEpoch = nextEpoch
        nextEpoch = nextEpoch + EPOCH_TERM
      }

    /* append probe status to history */
    case status: ProbeStatus =>
      getEpoch(status.timestamp) match {
        case Some(epoch) =>
          try {
            session.execute(bindInsertStatus(status, epoch))
          } catch {
            case ex: Throwable => log.debug("failed to insert status for {}: {}", status.probeRef, ex)
          }
        case None =>  // drop status
      }

    /* append notification to history */
    case notification: ProbeNotification =>
      getEpoch(notification.timestamp) match {
        case Some(epoch) =>
          try {
            session.execute(bindInsertNotification(notification, epoch))
          } catch {
            case ex: Throwable => log.debug("failed to insert status for {}: {}", notification.probeRef, ex)
          }
        case None =>  // drop notification
      }

    /* retrieve status history for the ProbeRef and all its children */
    case op: GetStatusHistory =>
      try {
        val epoch = op.from match {
          case Some(from) => timestamp2epoch(from)
          case None =>
            val resultSet = session.execute(bindGetFirstStatusEpoch(op.probeRef))
            if (!resultSet.isExhausted) resultSet.one().getLong(0) else -1
        }
        val resultSet = session.execute(bindGetStatusHistory(op.probeRef, epoch, op.from, op.limit.getOrElse(defaultLimit)))
        val statuses = resultSet.all().map(row2ProbeStatus)
        val matching = op.to match {
          case None => statuses
          case Some(to) =>
            statuses.filter(_.timestamp.isBefore(to))
        }
        sender() ! GetStatusHistoryResult(op, statuses.toVector)
      } catch {
        case ex: Throwable => log.debug("failed to get status history for {}: {}", op.probeRef, ex)
      }

    /* retrieve notification history for the ProbeRef and all its children */
    case op: GetNotificationHistory =>
      try {
        val epoch = op.from match {
          case Some(from) => timestamp2epoch(from)
          case None =>
            val resultSet = session.execute(bindGetFirstStatusEpoch(op.probeRef))
            if (!resultSet.isExhausted) resultSet.one().getLong(0) else -1
        }
        val resultSet = session.execute(bindGetNotificationHistory(op.probeRef, epoch, op.from, op.limit.getOrElse(defaultLimit)))
        val notifications = resultSet.all().map(row2ProbeNotification)
        val matching = op.to match {
          case None => notifications
          case Some(to) =>
            notifications.filter(_.timestamp.isBefore(to))
        }
        sender() ! GetNotificationHistoryResult(op, matching.toVector)
      } catch {
        case ex: Throwable => log.debug("failed to get notification history for {}: {}", op.probeRef, ex)
      }

    /* delete history older than statusHistoryAge */
    case CleanHistory(mark) =>
      //session.execute(bindCleanHistory(mark))
  }

  def getEpoch(timestamp: DateTime): Option[Long] = {
    val millis = timestamp.getMillis
    if (millis >= currEpoch && millis < nextEpoch)
      Some(currEpoch)
    else if (millis >= prevEpoch && millis < currEpoch)
      Some(prevEpoch)
    else if (millis >= nextEpoch && millis < currEpoch + EPOCH_TERM)
      Some(nextEpoch)
    else
      None
  }

  def millis2epoch(millis: Long): Long = (millis / EPOCH_TERM) * EPOCH_TERM

  def timestamp2epoch(timestamp: DateTime): Long = millis2epoch(timestamp.getMillis)

  private val preparedInsertStatus = session.prepare(insertStatusStatement)
  def bindInsertStatus(status: ProbeStatus, epoch: Long) = {
    new BoundStatement(preparedInsertStatus).bind(status.probeRef.toString, epoch: java.lang.Long,
      status.timestamp.toDate, status.lifecycle.toString, status.health.toString, status.summary.orNull,
      status.lastUpdate.map(_.toDate).orNull, status.lastChange.map(_.toDate).orNull, status.correlation.orNull,
      status.acknowledged.orNull, status.squelched: java.lang.Boolean)
  }

  private val preparedInsertNotification = session.prepare(insertNotificationStatement)
  def bindInsertNotification(notification: ProbeNotification, epoch: Long) = {
    new BoundStatement(preparedInsertNotification).bind(notification.probeRef.toString, epoch: java.lang.Long,
      notification.timestamp.toDate, notification.kind, notification.description, notification.correlation.orNull)
  }

  private val preparedCleanHistory = session.prepare(cleanStatusHistoryStatement)
  def bindCleanHistory(probeRef: ProbeRef, epoch: Long) = {
    new BoundStatement(preparedCleanHistory).bind(probeRef.toString, epoch: java.lang.Long)
  }

  private val preparedGetFirstStatusEpoch = session.prepare(getFirstStatusEpochStatement)
  def bindGetFirstStatusEpoch(probeRef: ProbeRef) = {
    new BoundStatement(preparedGetFirstStatusEpoch).bind(probeRef.toString)
  }

  private val preparedGetLastStatusEpoch = session.prepare(getLastStatusEpochStatement)
  def bindGetLastStatusEpoch(probeRef: ProbeRef) = {
    new BoundStatement(preparedGetLastStatusEpoch).bind(probeRef.toString)
  }

  private val preparedGetStatusHistory = session.prepare(getStatusHistoryStatement)
  def bindGetStatusHistory(probeRef: ProbeRef, epoch: Long, from: Option[DateTime], limit: Int) = {
    val start = from.map(_.toDate).getOrElse(SMALLEST_DATE)
    new BoundStatement(preparedGetStatusHistory)
      .bind(probeRef.toString, epoch: java.lang.Long, start, limit: java.lang.Integer)
  }

  private val preparedGetNotificationHistory = session.prepare(getNotificationHistoryStatement)
  def bindGetNotificationHistory(probeRef: ProbeRef, epoch: Long, from: Option[DateTime], limit: Int) = {
    val start = from.map(_.toDate).getOrElse(SMALLEST_DATE)
    new BoundStatement(preparedGetNotificationHistory)
      .bind(probeRef.toString, epoch: java.lang.Long, start, limit: java.lang.Integer)
  }

}

object CassandraArchiver {

  val LARGEST_DATE = new Date(java.lang.Long.MAX_VALUE)
  val SMALLEST_DATE = new Date(0)
  val EPOCH_TERM: Long = 60 * 60 * 24   // 1 day in seconds

  def props(managerSettings: CassandraArchiverSettings) = Props(classOf[CassandraArchiver], managerSettings)

  case class CassandraArchiverSettings()
  def settings(config: Config): Option[CassandraArchiverSettings] = {
    Some(CassandraArchiverSettings())
  }

  case object CheckEpoch
}
