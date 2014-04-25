package io.mandelbrot.core.history

import scala.slick.driver.H2Driver.simple._
import java.sql.{Date, Timestamp}
import java.util.UUID

sealed trait HistorySchema

/**
 *
 */
class StatusEntries(tag: Tag) extends Table[(String,String,String,Option[String],Option[String],Option[Long],Option[Long],Option[UUID],Option[UUID],Boolean)](tag, "StatusEntries") with HistorySchema {
  def probeRef = column[String]("probeRef")
  def lifecycle = column[String]("lifecycle")
  def health = column[String]("health")
  def summary = column[Option[String]]("summary")
  def detail = column[Option[String]]("detail")
  def lastUpdate = column[Option[Long]]("lastUpdate")
  def lastChange = column[Option[Long]]("lastChange")
  def correlation = column[Option[UUID]]("correlation")
  def acknowledged = column[Option[UUID]]("acknowledged")
  def squelched = column[Boolean]("squelched")
  def * = (probeRef, lifecycle, health, summary, detail, lastUpdate, lastChange, correlation, acknowledged, squelched)
}

object StatusEntries {
  type StatusEntry = (String,String,String,Option[String],Option[String],Option[Long],Option[Long],Option[UUID],Option[UUID],Boolean)
}

/**
 *
 */
class NotificationEntries(tag: Tag) extends Table[(String,Long,String,Option[UUID])](tag, "NotificationEntries") with HistorySchema {
  def probeRef = column[String]("probeRef")
  def timestamp = column[Long]("timestamp")
  def description = column[String]("description")
  def correlation = column[Option[UUID]]("correlation")
  def * = (probeRef, timestamp, description, correlation)
}

object NotificationEntries {
  type NotificationEntry = (String,Long,String,Option[UUID])
}