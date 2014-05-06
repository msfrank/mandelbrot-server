/**
 * Copyright 2014 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Mandelbrot.
 *
 * Mandelbrot is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Mandelbrot is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Mandelbrot.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mandelbrot.core.history

import com.typesafe.config.Config
import akka.actor.{Cancellable, Props, ActorLogging, Actor}
import scala.slick.driver.H2Driver.simple._
import scala.concurrent.duration._
import org.joda.time.{DateTimeZone, DateTime}
import java.io.File

import io.mandelbrot.core.registry._
import io.mandelbrot.core.ServerConfig
import io.mandelbrot.core.notification.ProbeNotification
import io.mandelbrot.core.registry.ProbeStatus

import io.mandelbrot.core.history.HistoryManager.ManagerSettings

/**
 *
 */
class HistoryManager(managerSettings: ManagerSettings) extends Actor with ActorLogging {
  import HistoryManager._
  import StatusEntries._
  import NotificationEntries._
  import context.dispatcher

  // config
  val settings = ServerConfig(context.system).settings.history
  val driver = "org.h2.Driver"
  val url = "jdbc:h2:" + {
    if (managerSettings.inMemory) "mem:history" else "file:" + managerSettings.databasePath.getAbsolutePath
  } + ";" + {
    if (managerSettings.inMemory) "DB_CLOSE_DELAY=-1;" else ""
  } + {
    if (!managerSettings.h2databaseToUpper) "DATABASE_TO_UPPER=false" else "DATABASE_TO_UPPER=true"
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
  val initialDelay = 30.seconds
  val interval = 5.minutes
  val historyCleaner: Option[Cancellable] = Some(context.system.scheduler.schedule(initialDelay, interval, self, CleanStaleHistory))

  def receive = {

    /* append probe status to history */
    case status: ProbeStatus =>
      db.withSession { implicit session =>
        val probeRef = status.probeRef.toString
        val timestamp = status.timestamp.getMillis
        val lifecycle = status.lifecycle.toString
        val health = status.health.toString
        val summary = status.summary
        val detail = status.detail
        val lastUpdate = status.lastUpdate.map(_.getMillis)
        val lastChange = status.lastChange.map(_.getMillis)
        val correlation = status.correlation
        val acknowledged = status.acknowledged
        val squelched = status.squelched
        statusEntries += ((probeRef, timestamp, lifecycle, health, summary, detail, lastUpdate, lastChange, correlation, acknowledged, squelched))
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
      val maxAge = DateTime.now(DateTimeZone.UTC).getMillis - settings.historyRetention.toMillis
      val staleStatus = statusEntries.filter(_.timestamp < maxAge).delete
      log.debug("cleaned {} records from statusEntries", staleStatus)
      val staleNotifications = notificationEntries.filter(_.timestamp < maxAge).delete
      log.debug("cleaned {} records from notificationEntries", staleNotifications)
  }

  def statusEntry2ProbeStatus(entry: StatusEntry): ProbeStatus = {
    val probeRef = ProbeRef(entry._1)
    val timestamp = new DateTime(entry._2)
    val lifecycle = entry._3 match {
      case "joining" => ProbeJoining
      case "known" => ProbeKnown
      case "leaving" => ProbeLeaving
      case "retired" => ProbeRetired
    }
    val health = entry._4 match {
      case "healthy" => ProbeHealthy
      case "degraded" => ProbeDegraded
      case "failed" => ProbeFailed
      case "unknown" => ProbeUnknown
    }
    val summary = entry._5
    val detail = entry._6
    val lastUpdate = entry._7.map(new DateTime(_))
    val lastChange = entry._8.map(new DateTime(_))
    val correlation = entry._9
    val acknowledged = entry._10
    val squelched = entry._11
    ProbeStatus(probeRef, timestamp, lifecycle, health, summary, detail, lastUpdate, lastChange, correlation, acknowledged, squelched)
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
  def props(managerSettings: ManagerSettings) = Props(classOf[HistoryManager], managerSettings)

  case class ManagerSettings(databasePath: File, inMemory: Boolean, h2databaseToUpper: Boolean)
  def settings(config: Config): Option[ManagerSettings] = {
    val databasePath = new File(config.getString("database-path"))
    val inMemory = config.getBoolean("in-memory")
    val h2databaseToUpper = config.getBoolean("h2-database-to-upper")
    Some(ManagerSettings(databasePath, inMemory, h2databaseToUpper))
  }

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
