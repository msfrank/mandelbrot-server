package io.mandelbrot.core.history

import akka.actor.{Cancellable, Props, ActorLogging, Actor}
import scala.slick.driver.H2Driver.simple._
import org.joda.time.DateTime
import java.sql.Date

import io.mandelbrot.core.registry.{ProbeStatus, ProbeRef}
import io.mandelbrot.core.ServerConfig
import io.mandelbrot.core.notification.{ProbeNotification, Notification}
import org.h2.jdbc.JdbcSQLException

/**
 *
 */
class HistoryManager extends Actor with ActorLogging {
  import HistoryManager._

  // config
  val settings = ServerConfig(context.system).settings.history
  val driver = "org.h2.Driver"
  val url = "jdbc:h2:" + {
    if (settings.inMemory) "mem:history" else "file:" + settings.databasePath.getAbsolutePath
  } + ";" + {
    if (settings.inMemory) "DB_CLOSE_DELAY=-1;" else ""
  } + {
    if (!settings.h2databaseToUpper) "DATABASE_TO_UPPER=false" else "DATABASE_TO_UPPER=true"
  }

  // initialize db
  val db = Database.forURL(url = url, driver = driver)
  val statusEntries = TableQuery[StatusEntries]
  val notificationEntries = TableQuery[NotificationEntries]
  implicit val session = db.createSession()

  // define (and possibly create) tables
  try {
    db.withSession(implicit session => (statusEntries.ddl ++ notificationEntries.ddl).create)
  } catch {
    case ex: Throwable => log.debug("skipping table creation: {}", ex.getMessage)
  }

  //
  val historyCleaner: Option[Cancellable] = None

  def receive = {

    /* append probe status to history */
    case status: ProbeStatus =>
      db.withSession { implicit session =>
        val probeRef = status.probeRef.toString
        val lifecycle = status.lifecycle.value
        val health = status.health.value
        val summary = status.summary
        val detail = status.detail
        val lastUpdate = status.lastUpdate.map(_.getMillis)
        val lastChange = status.lastChange.map(_.getMillis)
        val correlation = status.correlation
        val acknowledged = status.acknowledged
        val squelched = status.squelched
        statusEntries += ((probeRef, lifecycle, health, summary, detail, lastUpdate, lastChange, correlation, acknowledged, squelched))
      }

    /* append notification to history */
    case notification: ProbeNotification =>
      db.withSession { implicit session =>
        val probeRef = notification.probeRef.toString
        val timestamp = notification.timestamp.getMillis
        val description = notification.description
        val correlation = notification.correlation
        notificationEntries += ((probeRef, timestamp, description, correlation))
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
