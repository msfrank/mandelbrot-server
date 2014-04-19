package io.mandelbrot.core.state

import akka.actor.{Cancellable, Props, ActorLogging, Actor}
import org.joda.time.DateTime

import io.mandelbrot.core.ServerConfig
import io.mandelbrot.core.registry.{ProbeLifecycle, ProbeHealth, ProbeRef}
import io.mandelbrot.core.state.HistoryManager.CleanStaleHistory

/**
 *
 */
class HistoryManager extends Actor with ActorLogging {

  // config
  val settings = ServerConfig(context.system).settings.stateSettings

  // state
  val historyCleaner: Option[Cancellable] = None

  def receive = {

    /* append probe status to history */
    case update: UpdateProbeStatus =>
      log.debug("storing status {}", update)

    /* retrieve history for the ProbeRef and all its children */
    case query: GetAllHistory =>
      log.debug("received query {}", query)
      sender() ! GetAllHistoryResult(query, Vector.empty)

    /* retrieve history for the specified ProbeRefs only */
    case query: GetHistoryFor =>
      log.debug("received query {}", query)
      sender() ! GetHistoryForResult(query, Vector.empty)

    /* delete history older than statusHistoryAge */
    case CleanStaleHistory =>
  }
}

object HistoryManager {
  def props() = Props(classOf[HistoryManager])

  case object CleanStaleHistory
}

case class ProbeStatusHistory(probeRef: ProbeRef, timestamp: DateTime, lifecycle: ProbeLifecycle, health: ProbeHealth, summary: Option[String], detail: Option[String])

sealed trait HistoryServiceOperation
sealed trait HistoryServiceCommand extends HistoryServiceOperation
sealed trait HistoryServiceQuery extends HistoryServiceOperation
case class HistoryServiceOperationFailed(op: HistoryServiceOperation, failure: Throwable)

case class GetAllHistory(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends HistoryServiceQuery
case class GetAllHistoryResult(op: GetAllHistory, history: Vector[ProbeStatusHistory])

case class GetHistoryFor(probeRefs: Set[ProbeRef], from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends HistoryServiceQuery
case class GetHistoryForResult(op: GetHistoryFor, history: Vector[ProbeStatusHistory])

case class DeleteAllHistory(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime]) extends HistoryServiceCommand
case class DeleteHistoryFor(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime]) extends HistoryServiceCommand
