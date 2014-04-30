package io.mandelbrot.core.history

import akka.actor.{Cancellable, Props, ActorLogging, Actor}
import scala.slick.driver.H2Driver.simple._
import org.joda.time.DateTime

import io.mandelbrot.core.registry._
import io.mandelbrot.core.ServerConfig
import io.mandelbrot.core.notification.ProbeNotification
import io.mandelbrot.core.registry.ProbeStatus

/**
 *
 */
class HistoryManager extends Actor with ActorLogging {
  import HistoryManager._
  import StatusEntries._
  import NotificationEntries._

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
        val lifecycle = status.lifecycle.toString
        val health = status.health.toString
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
    case query @ GetStatusHistory(refs, from, to, limit) =>
      log.debug("retrieving status history: {}", query)
      var q = refs match {
        case Left(base) => statusEntries.filter(_.probeRef.startsWith(base.toString))
        case Right(refset) => statusEntries.filter(_.probeRef.inSet(refset.map(_.toString)))
      }
      if (limit.isDefined)
        q = q.take(limit.get)
      val results = q.list.toVector.map(statusEntry2ProbeStatus)
      sender() ! GetStatusHistoryResult(query, results)

    /* retrieve history for the ProbeRef and all its children */
    case query @ GetNotificationHistory(refs, from, to, limit) =>
      log.debug("retrieving notification history: {}", query)
      var q = refs match {
        case Left(base) => notificationEntries.filter(_.probeRef.startsWith(base.toString))
        case Right(refset) => notificationEntries.filter(_.probeRef.inSet(refset.map(_.toString)))
      }
      if (limit.isDefined)
        q = q.take(limit.get)
      val results = q.list.toVector.map(notificationEntry2ProbeNotification)
      sender() ! GetNotificationHistoryResult(query, results)

    /* delete history older than statusHistoryAge */
    case CleanStaleHistory =>
  }

  def statusEntry2ProbeStatus(entry: StatusEntry): ProbeStatus = {
    val probeRef = ProbeRef(entry._1)
    val lifecycle = entry._2 match {
      case "joining" => ProbeJoining
      case "known" => ProbeKnown
      case "leaving" => ProbeLeaving
      case "retired" => ProbeRetired
    }
    val health = entry._3 match {
      case "healthy" => ProbeHealthy
      case "degraded" => ProbeDegraded
      case "failed" => ProbeFailed
      case "unknown" => ProbeUnknown
    }
    val summary = entry._4
    val detail = entry._5
    val lastUpdate = entry._6.map(new DateTime(_))
    val lastChange = entry._7.map(new DateTime(_))
    val correlation = entry._8
    val acknowledged = entry._9
    val squelched = entry._10
    ProbeStatus(probeRef, lifecycle, health, summary, detail, lastUpdate, lastChange, correlation, acknowledged, squelched)
  }

  def notificationEntry2ProbeNotification(entry: NotificationEntry): ProbeNotification = {
    val probeRef = ProbeRef(entry._1)
    val timestamp = new DateTime(entry._2)
    val description = entry._3
    val correlation = entry._4
    ProbeNotification(probeRef, timestamp, description, correlation)
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

case class GetStatusHistory(refspec: Either[ProbeRef,Set[ProbeRef]], from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends HistoryServiceQuery
case class GetStatusHistoryResult(op: GetStatusHistory, history: Vector[ProbeStatus])

case class GetNotificationHistory(refspec: Either[ProbeRef,Set[ProbeRef]], from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends HistoryServiceQuery
case class GetNotificationHistoryResult(op: GetNotificationHistory, history: Vector[ProbeNotification])

case class DeleteAllHistory(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime]) extends HistoryServiceCommand
case class DeleteHistoryFor(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime]) extends HistoryServiceCommand
