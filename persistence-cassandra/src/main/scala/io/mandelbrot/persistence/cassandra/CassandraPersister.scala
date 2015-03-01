package io.mandelbrot.persistence.cassandra

import java.util.Date

import akka.actor.{Cancellable, Props, ActorLogging, Actor}
import akka.pattern.pipe
import com.typesafe.config.Config
import io.mandelbrot.core.system.{ProbeStatus, ProbeUnknown, ProbeJoining}
import io.mandelbrot.core.{ResourceNotFound, BadRequest, ApiException}
import org.joda.time.{DateTimeZone, DateTime}

import io.mandelbrot.core.state._
import io.mandelbrot.persistence.cassandra.CassandraPersister.CassandraPersisterSettings

import scala.concurrent.Future

/**
 *
 */
class CassandraPersister(settings: CassandraPersisterSettings) extends Actor with ActorLogging with Persister {
  import CassandraPersister._
  import context.dispatcher

  // config
  val defaultLimit = 100

  // state
  var currEpoch: Long = millis2epoch(System.currentTimeMillis())
  var prevEpoch: Long = currEpoch - EPOCH_TERM
  var nextEpoch: Long = currEpoch + EPOCH_TERM
  var checkEpoch: Option[Cancellable] = None

  val session = Cassandra(context.system).getSession
  val state = new StateDAL(settings, session)
  val notifications = new NotificationsDAL(settings, session)
  val conditions = new ConditionsDAL(settings, session)

  def receive = {

    case op: InitializeProbeStatus =>
      state.initializeProbeState(op.probeRef).flatMap {
        case _state: ProbeState =>
          val epoch = timestamp2epoch(_state.timestamp)
          conditions.getCondition(op.probeRef, epoch, _state.seqNum).recover {
            case ex: ApiException if ex.failure == ResourceNotFound =>
              ProbeCondition(DateTime.now(DateTimeZone.UTC), ProbeJoining, None, ProbeUnknown, None, None, false)
          }.map { condition =>
            val timestamp = condition.timestamp
            val lifecycle = condition.lifecycle
            val summary = condition.summary
            val health = condition.health
            val metrics = Map.empty[String,BigDecimal]
            val lastUpdate = _state.lastUpdate
            val lastChange = _state.lastChange
            val correlation = condition.correlation
            val acknowledged = condition.acknowledged
            val squelched = condition.squelched
            val status = ProbeStatus(timestamp, lifecycle, summary, health, metrics,
              lastUpdate, lastChange, correlation, acknowledged, squelched)
            InitializeProbeStatusResult(op, status, _state.seqNum)
          }
      }.recover {
        case ex: Throwable => StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: UpdateProbeStatus =>
      getEpoch(op.status.timestamp) match {
        case None =>
          sender() ! StateServiceOperationFailed(op, ApiException(BadRequest))
        case Some(epoch) =>
          val cFuture = conditions.insertCondition(op, epoch)
          val nFuture = notifications.insertNotifications(op, epoch)
          Future.sequence(Vector(cFuture,nFuture)).flatMap[Unit] { case _ =>
            val _state = ProbeState(op.probeRef, op.seqNum, op.status.timestamp, op.status.lastUpdate, op.status.lastChange)
            state.updateProbeState(_state)
          }.recover {
            case ex: Throwable => StateServiceOperationFailed(op, ex)
          }.pipeTo(sender())
      }

    case op: DeleteProbeStatus =>
      state.deleteProbeState(op.probeRef).recover {
        case ex: Throwable => StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())

//    /* retrieve status history for the ProbeRef and all its children */
//    case op: GetConditionHistory =>
//      val getEpoch = op.from match {
//        case Some(from) => Future.successful[Long](timestamp2epoch(from))
//        case None => statusHistory.getFirstStatusEpoch(op.probeRef).map[Long](_.getOrElse(-1))
//      }
//      getEpoch.flatMap[Vector[ProbeStatus]] { epoch =>
//        statusHistory.getStatusHistory(op.probeRef, epoch, op.from, op.to, op.limit.getOrElse(defaultLimit))
//      }.map { history => GetStatusHistoryResult(op, history) }.recover {
//        case ex: Throwable => HistoryServiceOperationFailed(op, ex)
//      }.pipeTo(sender())
//
//    /* retrieve notification history for the ProbeRef and all its children */
//    case op: GetNotificationHistory =>
//      val getEpoch = op.from match {
//        case Some(from) => Future.successful[Long](timestamp2epoch(from))
//        case None => notificationHistory.getFirstNotificationEpoch(op.probeRef).map[Long](_.getOrElse(-1))
//      }
//      getEpoch.flatMap[Vector[ProbeNotification]] { epoch =>
//        notificationHistory.getNotificationHistory(op.probeRef, epoch, op.from, op.to, op.limit.getOrElse(defaultLimit))
//      }.map { history => GetNotificationHistoryResult(op, history) }.recover {
//        case ex: Throwable => HistoryServiceOperationFailed(op, ex)
//      }.pipeTo(sender())
//
//    /* recalculate the epoch if necessary */
//    case CheckEpoch =>
//      val epoch = System.currentTimeMillis()
//      if (epoch >= nextEpoch) {
//        prevEpoch = currEpoch
//        currEpoch = nextEpoch
//        nextEpoch = nextEpoch + EPOCH_TERM
//      }
//
//    /* delete history older than statusHistoryAge */
//    case CleanHistory(mark) =>
//      session.execute(bindCleanHistory(mark))
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
}

object CassandraPersister {

  def props(managerSettings: CassandraPersisterSettings) = Props(classOf[CassandraPersister], managerSettings)

  val LARGEST_DATE = new Date(java.lang.Long.MAX_VALUE)
  val SMALLEST_DATE = new Date(0)
  val EPOCH_TERM: Long = 60 * 60 * 24   // 1 day in seconds

  case class CassandraPersisterSettings()
  def settings(config: Config): Option[CassandraPersisterSettings] = {
    Some(CassandraPersisterSettings())
  }

  case object CheckEpoch
}