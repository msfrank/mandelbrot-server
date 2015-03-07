package io.mandelbrot.persistence.cassandra

import java.util.Date

import akka.actor.{Cancellable, Props, ActorLogging, Actor}
import akka.pattern.pipe
import akka.serialization.SerializationExtension
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
  val session = Cassandra(context.system).getSession
  val serialization = SerializationExtension(context.system)
  val committedIndexDAL = new CommittedIndexDAL(settings, session, context.dispatcher)
  val probeStatusDAL = new ProbeStatusDAL(settings, session, context.dispatcher)

  def receive = {

    case op: InitializeProbeStatus =>
      committedIndexDAL.getCommittedIndex(op.probeRef).flatMap {
        case committedIndex =>
          val epoch = EpochUtils.timestamp2epoch(committedIndex.current)
          probeStatusDAL.getProbeStatus(op.probeRef, epoch, committedIndex.current).map {
            case status => InitializeProbeStatusResult(op, Some(status))
          }
      }.recover {
        case ex: ApiException if ex.failure == ResourceNotFound =>
          InitializeProbeStatusResult(op, None)
        case ex: Throwable =>
          StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: UpdateProbeStatus =>
      val commit = op.lastTimestamp match {
        case Some(last) =>
          committedIndexDAL.updateCommittedIndex(op.probeRef, op.status.timestamp, last)
        case None =>
          committedIndexDAL.initializeCommittedIndex(op.probeRef, op.status.timestamp)
      }
      val epoch = EpochUtils.timestamp2epoch(op.status.timestamp)
      commit.flatMap {
        case _ => probeStatusDAL.updateProbeStatus(op.probeRef, epoch, op.status, op.notifications)
      }.recover {
        case ex: Throwable => StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())

//    case op: DeleteProbeStatus =>
//      state.deleteProbeState(op.probeRef).recover {
//        case ex: Throwable => StateServiceOperationFailed(op, ex)
//      }.pipeTo(sender())

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