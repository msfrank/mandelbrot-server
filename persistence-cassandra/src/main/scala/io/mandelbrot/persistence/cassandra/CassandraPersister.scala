package io.mandelbrot.persistence.cassandra

import akka.actor.{Props, ActorLogging, Actor}
import akka.pattern.pipe
import com.typesafe.config.Config
import org.joda.time.DateTime

import io.mandelbrot.core.state._
import io.mandelbrot.core.model._
import io.mandelbrot.core._
import io.mandelbrot.persistence.cassandra.CassandraPersister.CassandraPersisterSettings

import scala.concurrent.Future

/**
 *
 */
class CassandraPersister(settings: CassandraPersisterSettings) extends Actor with ActorLogging with Persister {
  import context.dispatcher

  // config
  val defaultLimit = 100

  // state
  val session = Cassandra(context.system).getSession
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
      val probeRef = op.probeRef
      val from = op.from
      val to = op.to
      val last = op.last
      val limit = op.limit.getOrElse(defaultLimit)
      getEpoch(probeRef, from, to, last).flatMap {
        case (epoch, committed) =>
          probeStatusDAL.getProbeConditionHistory(probeRef, epoch, from, to, limit).map {
            case history => (epoch, committed, history)
          }
      }.map {
        case (epoch, committed, history) =>
          history.lastOption.map(_.timestamp) match {
            case Some(timestamp) =>
              if (history.length < limit && epoch == EpochUtils.timestamp2epoch(committed.current))
                GetConditionHistoryResult(op, ProbeConditionPage(history, Some(timestamp), exhausted = true))
              else
                GetConditionHistoryResult(op, ProbeConditionPage(history, Some(timestamp), exhausted = false))
            case None =>
              val last = Some(EpochUtils.epoch2timestamp(epoch))
              val exhausted = if (epoch == EpochUtils.timestamp2epoch(committed.current)) true else false
              GetConditionHistoryResult(op, ProbeConditionPage(history, last, exhausted))
          }
      }.recover {
        case ex: Throwable => StateServiceOperationFailed(op, ex)
      }.pipeTo(sender())

//    /* retrieve notification history for the specified ProbeRef */
//    case op: GetNotificationHistory =>
  }

  /**
   * determine which epoch to read from, given the specified constraints.
   */
  def getEpoch(probeRef: ProbeRef,
               from: Option[DateTime],
               to: Option[DateTime],
               last: Option[DateTime]): Future[(Long,CommittedIndex)] = {
    committedIndexDAL.getCommittedIndex(probeRef).flatMap[(Long,CommittedIndex)] { committed =>
      last match {
        // if last was specified, then return the epoch derived from the last timestamp
        case Some(timestamp) =>
          val epoch = EpochUtils.timestamp2epoch(timestamp)
          probeStatusDAL.checkIfEpochExhausted(probeRef, epoch, timestamp).map {
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

object CassandraPersister {

  def props(managerSettings: CassandraPersisterSettings) = Props(classOf[CassandraPersister], managerSettings)

  case class CassandraPersisterSettings()
  def settings(config: Config): Option[CassandraPersisterSettings] = {
    Some(CassandraPersisterSettings())
  }
}