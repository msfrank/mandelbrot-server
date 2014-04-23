package io.mandelbrot.core.history

import scala.slick.driver.H2Driver.simple._
import java.sql.Timestamp
import java.util.UUID

sealed trait HistorySchema

/**
 *
 */
class StatusEntries(tag: Tag) extends Table[(Long,Timestamp,String,Option[UUID],String)](tag, "StatusEntries") with HistorySchema {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
  def timestamp = column[Timestamp]("timestamp")
  def entryType = column[String]("entryType")
  def correlationId = column[Option[UUID]]("correlationId")
  def message = column[String]("message")
  def * = (id, timestamp, entryType, correlationId, message)
}