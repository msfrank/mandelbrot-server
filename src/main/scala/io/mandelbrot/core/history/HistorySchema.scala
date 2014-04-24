package io.mandelbrot.core.history

import scala.slick.driver.H2Driver.simple._
import java.sql.{Date, Timestamp}
import java.util.UUID

sealed trait HistorySchema

/**
 *
 */
class StatusEntries(tag: Tag) extends Table[(String,Long,String,String,Option[String],Option[String],Option[UUID])](tag, "StatusEntries") with HistorySchema {
  def probeRef = column[String]("probeRef")
  def timestamp = column[Long]("timestamp")
  def lifecycle = column[String]("lifecycle")
  def health = column[String]("health")
  def summary = column[Option[String]]("summary")
  def detail = column[Option[String]]("detail")
  def correlationId = column[Option[UUID]]("correlationId")
  def * = (probeRef, timestamp, lifecycle, health, summary, detail, correlationId)
}

/**
 *
 */
class NotificationEntries(tag: Tag) extends Table[(String,Long,String,Option[UUID])](tag, "NotificationEntries") with HistorySchema {
  def probeRef = column[String]("probeRef")
  def timestamp = column[Long]("timestamp")
  def description = column[String]("description")
  def correlationId = column[Option[UUID]]("correlationId")
  def * = (probeRef, timestamp, description, correlationId)
}