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
import akka.actor.{Props, ActorLogging, Actor}
import io.mandelbrot.core.system.ProbeRef
import scala.slick.driver.JdbcProfile
import scala.slick.jdbc.JdbcBackend.Database
import org.joda.time.DateTime
import java.io.File
import java.util.UUID

import io.mandelbrot.core.ServerConfig
import io.mandelbrot.core.registry._
import io.mandelbrot.core.notification.ProbeNotification
import io.mandelbrot.core.registry.ProbeStatus

import io.mandelbrot.core.history.H2Archiver.H2ArchiverSettings

/*
 this multi-db code is heavily inspired by
 https://github.com/slick/slick-examples/blob/master/src/main/scala/com/typesafe/slick/examples/lifted/MultiDBCakeExample.scala
 */

trait Profile {
  val profile: JdbcProfile
}
/**
 *
 */
trait StatusEntriesComponent { this: Profile =>
  import profile.simple._
  import StatusEntries._

  class StatusEntries(tag: Tag) extends Table[(String,Long,String,String,Option[String],Option[Long],Option[Long],Option[UUID],Option[UUID],Boolean)](tag, "status_entries") {
    def probeRef = column[String]("probeRef")
    def timestamp = column[Long]("timestamp")
    def lifecycle = column[String]("lifecycle")
    def health = column[String]("health")
    def summary = column[Option[String]]("summary")
    def lastUpdate = column[Option[Long]]("lastUpdate")
    def lastChange = column[Option[Long]]("lastChange")
    def correlation = column[Option[UUID]]("correlation")
    def acknowledged = column[Option[UUID]]("acknowledged")
    def squelched = column[Boolean]("squelched")
    def * = (probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlation, acknowledged, squelched)
  }

  val statusEntries = TableQuery[StatusEntries]

  def insert(status: ProbeStatus)(implicit session: Session): Unit = {
    val probeRef = status.probeRef.toString
    val timestamp = status.timestamp.getMillis
    val lifecycle = status.lifecycle.toString
    val health = status.health.toString
    val summary = status.summary
    val lastUpdate = status.lastUpdate.map(_.getMillis)
    val lastChange = status.lastChange.map(_.getMillis)
    val correlation = status.correlation
    val acknowledged = status.acknowledged
    val squelched = status.squelched
    statusEntries += ((probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlation, acknowledged, squelched))
  }

  def query(query: GetStatusHistory)(implicit session: Session): Vector[ProbeStatus] = {
    var q = query.refspec match {
      case Left(base) => statusEntries.filter(_.probeRef.startsWith(base.toString))
      case Right(refset) => statusEntries.filter(_.probeRef.inSet(refset.map(_.toString)))
    }
    for (millis <- query.from.map(_.getMillis))
      q = q.filter(_.timestamp > millis)
    for (millis <- query.to.map(_.getMillis))
      q = q.filter(_.timestamp < millis)
    if (query.limit.isDefined)
      q = q.take(query.limit.get)
    //log.debug("{} expands to SQL query '{}'", query, q.selectStatement)
    q.list.toVector.map(statusEntry2ProbeStatus)
  }


  object StatusEntries {
    type StatusEntry = (String,Long,String,String,Option[String],Option[Long],Option[Long],Option[UUID],Option[UUID],Boolean)

    def statusEntry2ProbeStatus(entry: StatusEntry): ProbeStatus = {
      val probeRef = ProbeRef(entry._1)
      val timestamp = new DateTime(entry._2)
      val lifecycle = entry._3 match {
        case "initializing" => ProbeInitializing
        case "joining" => ProbeJoining
        case "known" => ProbeKnown
        case "synthetic" => ProbeSynthetic
        case "retired" => ProbeRetired
      }
      val health = entry._4 match {
        case "healthy" => ProbeHealthy
        case "degraded" => ProbeDegraded
        case "failed" => ProbeFailed
        case "unknown" => ProbeUnknown
      }
      val summary = entry._5
      val lastUpdate = entry._6.map(new DateTime(_))
      val lastChange = entry._7.map(new DateTime(_))
      val correlation = entry._8
      val acknowledged = entry._9
      val squelched = entry._10
      ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlation, acknowledged, squelched)
    }
  }
}

/**
 *
 */
trait NotificationEntriesComponent { this: Profile =>
  import profile.simple._
  import NotificationEntries._

  class NotificationEntries(tag: Tag) extends Table[(String,Long,String,String,Option[UUID])](tag, "notification_entries") {
    def probeRef = column[String]("probeRef")
    def timestamp = column[Long]("timestamp")
    def kind = column[String]("kind")
    def description = column[String]("description")
    def correlation = column[Option[UUID]]("correlation")
    def * = (probeRef, timestamp, kind, description, correlation)
  }

  val notificationEntries = TableQuery[NotificationEntries]

  def insert(notification: ProbeNotification)(implicit session: Session): Unit = {
    val probeRef = notification.probeRef.toString
    val timestamp = notification.timestamp.getMillis
    val kind = notification.kind
    val description = notification.description
    val correlation = notification.correlation
    notificationEntries += ((probeRef, timestamp, kind, description, correlation))
  }

  def query(query: GetNotificationHistory)(implicit session: Session): Vector[ProbeNotification] = {
    var q = query.refspec match {
      case Left(base) => notificationEntries.filter(_.probeRef.startsWith(base.toString))
      case Right(refset) => notificationEntries.filter(_.probeRef.inSet(refset.map(_.toString)))
    }
    for (millis <- query.from.map(_.getMillis))
      q = q.filter(_.timestamp > millis)
    for (millis <- query.to.map(_.getMillis))
      q = q.filter(_.timestamp < millis)
    for (limit <- query.limit)
      q = q.take(limit)
    //log.debug("{} expands to SQL query '{}'", query, q.selectStatement)
    q.list.toVector.map(notificationEntry2ProbeNotification)
  }

  object NotificationEntries {
    type NotificationEntry = (String,Long,String,String,Option[UUID])

    def notificationEntry2ProbeNotification(entry: NotificationEntry): ProbeNotification = {
      val probeRef = ProbeRef(entry._1)
      val timestamp = new DateTime(entry._2)
      val kind = entry._3
      val description = entry._4
      val correlation = entry._5
      ProbeNotification(probeRef, timestamp, kind, description, correlation)
    }
  }
}

/**
 *
 */
class DAL(override val profile: JdbcProfile) extends StatusEntriesComponent with NotificationEntriesComponent with Profile {
  import profile.simple._

  def create(implicit session: Session): Unit = {
    try {
      statusEntries.ddl.create
    } catch {
      case ex: Throwable =>
    }
    try {
      notificationEntries.ddl.create
    } catch {
      case ex: Throwable =>
    }
  }

  def clean(mark: DateTime)(implicit session: Session): Unit = {
    val staleStatus = statusEntries.filter(_.timestamp < mark.getMillis).delete
    //log.debug("cleaned {} records from statusEntries", staleStatus)
    val staleNotifications = notificationEntries.filter(_.timestamp < mark.getMillis).delete
    //log.debug("cleaned {} records from notificationEntries", staleNotifications)
  }
}


/**
 *
 */
trait SlickArchiver extends Actor with ActorLogging {
  import scala.slick.jdbc.JdbcBackend.Database

  // abstract members
  val db: Database
  val dal: DAL

  // config
  val settings = ServerConfig(context.system).settings.history

  def receive = {

    /* append probe status to history */
    case status: ProbeStatus =>
      db.withSession { implicit session => dal.insert(status) }

    /* append notification to history */
    case notification: ProbeNotification =>
      db.withSession { implicit session => dal.insert(notification) }

    /* retrieve status history for the ProbeRef and all its children */
    case query: GetStatusHistory =>
      db.withSession { implicit session =>
        val results = dal.query(query)
        sender() ! GetStatusHistoryResult(query, results)
      }

    /* retrieve notification history for the ProbeRef and all its children */
    case query: GetNotificationHistory =>
      db.withSession { implicit session =>
        val results = dal.query(query)
        sender() ! GetNotificationHistoryResult(query, results)
      }

    /* delete history older than statusHistoryAge */
    case CleanHistory(mark) =>
      db.withSession { implicit session => dal.clean(mark) }
  }
}

/**
 *
 */
class H2Archiver(managerSettings: H2ArchiverSettings) extends SlickArchiver with Archiver {
  import scala.slick.driver.H2Driver

   // config
  val url = "jdbc:h2:" + {
    if (managerSettings.inMemory) "mem:history" else "file:" + managerSettings.databasePath.getAbsolutePath
  } + ";" + {
    if (managerSettings.inMemory) "DB_CLOSE_DELAY=-1;" else ""
  } + {
    if (!managerSettings.h2databaseToUpper) "DATABASE_TO_UPPER=false" else "DATABASE_TO_UPPER=true"
  }

  val db = Database.forURL(url = url, driver = "org.h2.Driver")
  val dal = new DAL(H2Driver)

  db.withSession { implicit session => dal.create }
}

object H2Archiver {
  def props(managerSettings: H2ArchiverSettings) = Props(classOf[H2Archiver], managerSettings)

  case class H2ArchiverSettings(databasePath: File, inMemory: Boolean, h2databaseToUpper: Boolean)
  def settings(config: Config): Option[H2ArchiverSettings] = {
    val databasePath = new File(config.getString("database-path"))
    val inMemory = config.getBoolean("in-memory")
    val h2databaseToUpper = config.getBoolean("h2-database-to-upper")
    Some(H2ArchiverSettings(databasePath, inMemory, h2databaseToUpper))
  }
}

