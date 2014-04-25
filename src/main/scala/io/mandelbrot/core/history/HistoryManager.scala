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
    case query @ GetStatusHistory(Left(ref), from, to, limit) =>
      var q = statusEntries.filter(_.probeRef.startsWith(ref.toString))
      if (limit.isDefined)
        q = q.take(limit.get)
      val results = q.list.toVector.map { case tuple =>
        val probeRef = ProbeRef(tuple._1)
        val lifecycle = tuple._2 match {
          case "joining" => ProbeJoining
          case "known" => ProbeKnown
          case "leaving" => ProbeLeaving
          case "retired" => ProbeRetired
        }
        val health = tuple._3 match {
          case "healthy" => ProbeHealthy
          case "degraded" => ProbeDegraded
          case "failed" => ProbeFailed
          case "unknown" => ProbeUnknown
        }
        val summary = tuple._4
        val detail = tuple._5
        val lastUpdate = tuple._6.map(new DateTime(_))
        val lastChange = tuple._7.map(new DateTime(_))
        val correlation = tuple._8
        val acknowledged = tuple._9
        val squelched = tuple._10
        ProbeStatus(probeRef, lifecycle, health, summary, detail, lastUpdate, lastChange, correlation, acknowledged, squelched)
      }
      sender() ! GetStatusHistoryResult(query, results)

    /* retrieve history for the ProbeRef and all its children */
    case query @ GetStatusHistory(Right(refs), from, to, limit) =>
      sender() ! GetStatusHistoryResult(query, Vector.empty)

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

case class GetStatusHistory(refspec: Either[ProbeRef,Set[ProbeRef]], from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends HistoryServiceQuery
case class GetStatusHistoryResult(op: GetStatusHistory, history: Vector[ProbeStatus])

case class DeleteAllHistory(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime]) extends HistoryServiceCommand
case class DeleteHistoryFor(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime]) extends HistoryServiceCommand
