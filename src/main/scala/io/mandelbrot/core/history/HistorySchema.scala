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

import scala.slick.driver.H2Driver.simple._
import java.sql.{Date, Timestamp}
import java.util.UUID

sealed trait HistorySchema

/**
 *
 */
class StatusEntries(tag: Tag) extends Table[(String,Long,String,String,Option[String],Option[Long],Option[Long],Option[UUID],Option[UUID],Boolean)](tag, "StatusEntries") with HistorySchema {
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

object StatusEntries {
  type StatusEntry = (String,Long,String,String,Option[String],Option[Long],Option[Long],Option[UUID],Option[UUID],Boolean)
}

/**
 *
 */
class NotificationEntries(tag: Tag) extends Table[(String,Long,String,String,Option[UUID])](tag, "NotificationEntries") with HistorySchema {
  def probeRef = column[String]("probeRef")
  def timestamp = column[Long]("timestamp")
  def kind = column[String]("kind")
  def description = column[String]("description")
  def correlation = column[Option[UUID]]("correlation")
  def * = (probeRef, timestamp, kind, description, correlation)
}

object NotificationEntries {
  type NotificationEntry = (String,Long,String,String,Option[UUID])
}