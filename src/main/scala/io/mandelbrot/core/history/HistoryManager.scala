package io.mandelbrot.core.history

import akka.actor.{Cancellable, Props, ActorLogging, Actor}
import scala.slick.driver.H2Driver.simple._
import org.joda.time.DateTime
import java.sql.Date

import io.mandelbrot.core.registry.ProbeRef
import io.mandelbrot.core.state.UpdateProbeStatus
import io.mandelbrot.core.ServerConfig

/**
 *
 */
class HistoryManager extends Actor with ActorLogging {
  import HistoryManager._

  // config
  val settings = ServerConfig(context.system).settings.history
  val url = "jdbc:h2:mem:history;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1"
  val driver = "org.h2.Driver"

  // initialize db
  val db = Database.forURL(url = url, driver = driver)
  val statusEntries = TableQuery[StatusEntries]
  val notificationEntries = TableQuery[NotificationEntries]
  val historyCleaner: Option[Cancellable] = None

  // define tables
  db.withSession { implicit session =>
    statusEntries.ddl.create
    notificationEntries.ddl.create
  }

  def receive = {

    /* append probe status to history */
    case update @ UpdateProbeStatus(probeRef, timestamp, lifecycle, health, summary, detail) =>
      log.debug("received {}", update)
      db.withSession { implicit session =>
        statusEntries += ((probeRef.toString, new Date(timestamp.getMillis), lifecycle.value, health.value, summary, detail, None))
      }

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

/* */
sealed trait HistoryServiceOperation
sealed trait HistoryServiceCommand extends HistoryServiceOperation
sealed trait HistoryServiceQuery extends HistoryServiceOperation
case class HistoryServiceOperationFailed(op: HistoryServiceOperation, failure: Throwable)

case class GetAllHistory(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends HistoryServiceQuery
case class GetAllHistoryResult(op: GetAllHistory, history: Vector[StatusEntries])

case class GetHistoryFor(probeRefs: Set[ProbeRef], from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends HistoryServiceQuery
case class GetHistoryForResult(op: GetHistoryFor, history: Vector[StatusEntries])

case class DeleteAllHistory(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime]) extends HistoryServiceCommand
case class DeleteHistoryFor(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime]) extends HistoryServiceCommand
