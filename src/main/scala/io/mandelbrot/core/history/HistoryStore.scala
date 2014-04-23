package io.mandelbrot.core.history

import akka.actor.{Cancellable, Props, ActorLogging, Actor}
import scala.slick.driver.H2Driver.simple._
import org.joda.time.DateTime

import io.mandelbrot.core.ServerConfig
import io.mandelbrot.core.registry.{ProbeLifecycle, ProbeHealth, ProbeRef}
import io.mandelbrot.core.state.UpdateProbeStatus

/**
 *
 */
class HistoryStore extends Actor with ActorLogging {
  import HistoryStore._

  // config
  val settings = ServerConfig(context.system).settings.history
  val url = "jdbc:h2:mem:history"
  val driver = "org.h2.driver"

  // state
  val db = Database.forURL(url = url, driver = driver)
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

object HistoryStore {
  def props() = Props(classOf[HistoryStore])

  case object CleanStaleHistory
}

case class ProbeStatusHistory(probeRef: ProbeRef, timestamp: DateTime, lifecycle: ProbeLifecycle, health: ProbeHealth, summary: Option[String], detail: Option[String])

/* */
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
