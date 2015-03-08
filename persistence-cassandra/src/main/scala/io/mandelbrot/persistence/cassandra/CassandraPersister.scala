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
        case ex: Throwable => StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    case op: UpdateProbeStatus =>
      val commit = op.lastTimestamp match {
        case Some(last) =>
          committedIndexDAL.updateCommittedIndex(op.probeRef, op.status.timestamp, last)
        case None =>
          committedIndexDAL.initializeCommittedIndex(op.probeRef, op.status.timestamp)
      }
      val epoch = EpochUtils.timestamp2epoch(op.status.timestamp)
      commit.flatMap[UpdateProbeStatusResult] { _ =>
        probeStatusDAL.updateProbeStatus(op.probeRef, epoch, op.status, op.notifications).map {
          _ => UpdateProbeStatusResult(op)
        }
      }.recover {
        case ex: Throwable => StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())

    /* retrieve condition history for the specified ProbeRef */
    case op: GetConditionHistory =>
      val getEpoch: Future[Long] = op.from match {
        case _ if op.last.nonEmpty =>
          val last = op.last.get
          val epoch = EpochUtils.timestamp2epoch(last)
          probeStatusDAL.checkIfEpochExhausted(op.probeRef, epoch, last).map {
            case true => epoch
            case false => EpochUtils.nextEpoch(epoch)
          }
        case Some(from) =>
          Future.successful[Long](EpochUtils.timestamp2epoch(from))
        case None =>
          committedIndexDAL.getCommittedIndex(op.probeRef).map {
            committedIndex => EpochUtils.timestamp2epoch(committedIndex.initial)
          }
      }
      val limit = op.limit.getOrElse(defaultLimit)
      getEpoch.flatMap[(Long,Vector[ProbeCondition])] { epoch =>
        probeStatusDAL.getProbeConditionHistory(op.probeRef, epoch, op.from, op.to, op.limit.getOrElse(defaultLimit))
      }.map {
        case (epoch, history) if history.nonEmpty =>
          val last = history.last.timestamp
          val exhausted = if (history.length < limit && atHorizon(last, op.to)) true else false
          GetConditionHistoryResult(op, history, Some(last), exhausted)
        case (epoch, history) =>
          val last = Some(EpochUtils.epoch2timestamp(epoch))
          val exhausted = if (atHorizon(epoch, op.to)) true else false
          GetConditionHistoryResult(op, history, last, exhausted)
      }.recover {
        case ex: Throwable => StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())

//    /* retrieve notification history for the specified ProbeRef */
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
//    case op: DeleteProbeStatus =>
//      state.deleteProbeState(op.probeRef).recover {
//        case ex: Throwable => StateServiceOperationFailed(op, ex)
//      }.pipeTo(sender())
  }

  def atHorizon(epoch: Long, horizon: Option[DateTime]): Boolean = {
    val horizonEpoch = EpochUtils.timestamp2epoch(horizon.getOrElse(new DateTime(DateTimeZone.UTC)))
    epoch == horizonEpoch
  }

  def atHorizon(timestamp: DateTime, horizon: Option[DateTime]): Boolean = {
    atHorizon(EpochUtils.timestamp2epoch(timestamp), horizon)
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