package io.mandelbrot.persistence.cassandra

import java.util.Date

import akka.actor.{Cancellable, Props, ActorLogging, Actor}
import akka.pattern.pipe
import com.typesafe.config.Config
import org.joda.time.DateTime

import io.mandelbrot.core.state._
import io.mandelbrot.persistence.cassandra.CassandraPersister.CassandraPersisterSettings

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

  def receive = {

    case op: InitializeProbeStatus =>
      state.initializeProbeState(op).recover {
        case ex: Throwable => sender() ! StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: UpdateProbeStatus =>
      state.updateProbeState(op).recover {
        case ex: Throwable => sender() ! StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: DeleteProbeStatus =>
      state.deleteProbeState(op).recover {
        case ex: Throwable => sender() ! StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    /* append probe status to history */
    case status: ProbeStatus =>
      getEpoch(status.timestamp) match {
        case Some(epoch) => statusHistory.insertStatus(status, epoch)
        case None =>  // drop status
      }

    /* append notification to history */
    case notification: ProbeNotification =>
      getEpoch(notification.timestamp) match {
        case Some(epoch) => notificationHistory.insertNotification(notification, epoch)
        case None =>  // drop notification
      }

    /* retrieve status history for the ProbeRef and all its children */
    case op: GetStatusHistory =>
      val getEpoch = op.from match {
        case Some(from) => Future.successful[Long](timestamp2epoch(from))
        case None => statusHistory.getFirstStatusEpoch(op.probeRef).map[Long](_.getOrElse(-1))
      }
      getEpoch.flatMap[Vector[ProbeStatus]] { epoch =>
        statusHistory.getStatusHistory(op.probeRef, epoch, op.from, op.to, op.limit.getOrElse(defaultLimit))
      }.map { history => GetStatusHistoryResult(op, history) }.recover {
        case ex: Throwable => HistoryServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    /* retrieve notification history for the ProbeRef and all its children */
    case op: GetNotificationHistory =>
      val getEpoch = op.from match {
        case Some(from) => Future.successful[Long](timestamp2epoch(from))
        case None => notificationHistory.getFirstNotificationEpoch(op.probeRef).map[Long](_.getOrElse(-1))
      }
      getEpoch.flatMap[Vector[ProbeNotification]] { epoch =>
        notificationHistory.getNotificationHistory(op.probeRef, epoch, op.from, op.to, op.limit.getOrElse(defaultLimit))
      }.map { history => GetNotificationHistoryResult(op, history) }.recover {
        case ex: Throwable => HistoryServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    /* recalculate the epoch if necessary */
    case CheckEpoch =>
      val epoch = System.currentTimeMillis()
      if (epoch >= nextEpoch) {
        prevEpoch = currEpoch
        currEpoch = nextEpoch
        nextEpoch = nextEpoch + EPOCH_TERM
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