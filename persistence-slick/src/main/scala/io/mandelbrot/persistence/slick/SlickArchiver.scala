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

package io.mandelbrot.persistence.slick

import com.typesafe.config.Config
import akka.actor.{Props, ActorLogging, Actor}
import scala.slick.driver.JdbcProfile
import scala.slick.jdbc.JdbcBackend.Database
import org.joda.time.DateTime
import java.io.File
import java.util.UUID

import io.mandelbrot.core.ServerConfig
import io.mandelbrot.core.check._
import io.mandelbrot.core.history._
import io.mandelbrot.core.notification._
import io.mandelbrot.persistence.slick.H2Archiver.H2ArchiverSettings

/*
 this multi-db code is heavily inspired by
 https://github.com/slick/slick-examples/blob/master/src/main/scala/com/typesafe/slick/examples/lifted/MultiDBCakeExample.scala
 */

trait ArchiverProfile {
  val profile: JdbcProfile
}
/**
 *
 */
trait StatusEntriesComponent { this: ArchiverProfile =>
  import profile.simple._
  import StatusEntries._

  class StatusEntries(tag: Tag) extends Table[(String,Long,String,String,Option[String],Option[Long],Option[Long],Option[UUID],Option[UUID],Boolean)](tag, "status_entries") {
    def checkRef = column[String]("checkRef")
    def timestamp = column[Long]("timestamp")
    def lifecycle = column[String]("lifecycle")
    def health = column[String]("health")
    def summary = column[Option[String]]("summary")
    def lastUpdate = column[Option[Long]]("lastUpdate")
    def lastChange = column[Option[Long]]("lastChange")
    def correlation = column[Option[UUID]]("correlation")
    def acknowledged = column[Option[UUID]]("acknowledged")
    def squelched = column[Boolean]("squelched")
    def * = (checkRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlation, acknowledged, squelched)
  }

  val statusEntries = TableQuery[StatusEntries]

  def insert(status: CheckStatus)(implicit session: Session): Unit = {
    val checkRef = status.checkRef.toString
    val timestamp = status.timestamp.getMillis
    val lifecycle = status.lifecycle.toString
    val health = status.health.toString
    val summary = status.summary
    val lastUpdate = status.lastUpdate.map(_.getMillis)
    val lastChange = status.lastChange.map(_.getMillis)
    val correlation = status.correlation
    val acknowledged = status.acknowledged
    val squelched = status.squelched
    statusEntries += ((checkRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlation, acknowledged, squelched))
  }

  def query(query: GetStatusHistory)(implicit session: Session): Vector[CheckStatus] = {
    val checkRef = query.checkRef.toString
    var q = statusEntries.filter(_.checkRef === checkRef)
    for (millis <- query.from.map(_.getMillis))
      q = q.filter(_.timestamp > millis)
    for (millis <- query.to.map(_.getMillis))
      q = q.filter(_.timestamp < millis)
    if (query.limit.isDefined)
      q = q.take(query.limit.get)
    //log.debug("{} expands to SQL query '{}'", query, q.selectStatement)
    q.list.toVector.map(statusEntry2CheckStatus)
  }


  object StatusEntries {
    type StatusEntry = (String,Long,String,String,Option[String],Option[Long],Option[Long],Option[UUID],Option[UUID],Boolean)

    def statusEntry2CheckStatus(entry: StatusEntry): CheckStatus = {
      val checkRef = CheckRef(entry._1)
      val timestamp = new DateTime(entry._2)
      val lifecycle = entry._3 match {
        case "initializing" => CheckInitializing
        case "joining" => CheckJoining
        case "known" => CheckKnown
        case "synthetic" => CheckSynthetic
        case "retired" => CheckRetired
      }
      val health = entry._4 match {
        case "healthy" => CheckHealthy
        case "degraded" => CheckDegraded
        case "failed" => CheckFailed
        case "unknown" => CheckUnknown
      }
      val summary = entry._5
      val lastUpdate = entry._6.map(new DateTime(_))
      val lastChange = entry._7.map(new DateTime(_))
      val correlation = entry._8
      val acknowledged = entry._9
      val squelched = entry._10
      CheckStatus(checkRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlation, acknowledged, squelched)
    }
  }
}

/**
 *
 */
trait NotificationEntriesComponent { this: ArchiverProfile =>
  import profile.simple._
  import NotificationEntries._

  class NotificationEntries(tag: Tag) extends Table[(String,Long,String,String,Option[UUID])](tag, "notification_entries") {
    def checkRef = column[String]("checkRef")
    def timestamp = column[Long]("timestamp")
    def kind = column[String]("kind")
    def description = column[String]("description")
    def correlation = column[Option[UUID]]("correlation")
    def * = (checkRef, timestamp, kind, description, correlation)
  }

  val notificationEntries = TableQuery[NotificationEntries]

  def insert(notification: CheckNotification)(implicit session: Session): Unit = {
    val checkRef = notification.checkRef.toString
    val timestamp = notification.timestamp.getMillis
    val kind = notification.kind
    val description = notification.description
    val correlation = notification.correlation
    notificationEntries += ((checkRef, timestamp, kind, description, correlation))
  }

  def query(query: GetNotificationHistory)(implicit session: Session): Vector[CheckNotification] = {
    val checkRef = query.checkRef.toString
    var q = notificationEntries.filter(_.checkRef === checkRef)
    for (millis <- query.from.map(_.getMillis))
      q = q.filter(_.timestamp > millis)
    for (millis <- query.to.map(_.getMillis))
      q = q.filter(_.timestamp < millis)
    for (limit <- query.limit)
      q = q.take(limit)
    //log.debug("{} expands to SQL query '{}'", query, q.selectStatement)
    q.list.toVector.map(notificationEntry2CheckNotification)
  }

  object NotificationEntries {
    type NotificationEntry = (String,Long,String,String,Option[UUID])

    def notificationEntry2CheckNotification(entry: NotificationEntry): CheckNotification = {
      val checkRef = CheckRef(entry._1)
      val timestamp = new DateTime(entry._2)
      val kind = entry._3
      val description = entry._4
      val correlation = entry._5
      CheckNotification(checkRef, timestamp, kind, description, correlation)
    }
  }
}

/**
 *
 */
class ArchiveDAL(override val profile: JdbcProfile) extends StatusEntriesComponent with NotificationEntriesComponent with ArchiverProfile {
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
  val dal: ArchiveDAL

  // config
  val settings = ServerConfig(context.system).settings.history

  def receive = {

    /* append check status to history */
    case status: CheckStatus =>
      db.withSession { implicit session => dal.insert(status) }

    /* append notification to history */
    case notification: CheckNotification =>
      db.withSession { implicit session => dal.insert(notification) }

    /* retrieve status history for the CheckRef and all its children */
    case query: GetStatusHistory =>
      db.withSession { implicit session =>
        val results = dal.query(query)
        sender() ! GetStatusHistoryResult(query, results)
      }

    /* retrieve notification history for the CheckRef and all its children */
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
    if (managerSettings.inMemory) "mem:history" else "file:" + managerSettings.databasePath.getAbsolutePath + "/history"
  } + ";" + {
    if (managerSettings.inMemory) "DB_CLOSE_DELAY=-1;" else ""
  } + {
    if (!managerSettings.h2databaseToUpper) "DATABASE_TO_UPPER=false" else "DATABASE_TO_UPPER=true"
  }

  val db = Database.forURL(url = url, driver = "org.h2.Driver")
  val dal = new ArchiveDAL(H2Driver)

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

