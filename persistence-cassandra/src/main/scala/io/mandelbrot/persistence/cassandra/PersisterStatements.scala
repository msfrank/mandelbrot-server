package io.mandelbrot.persistence.cassandra

import akka.util.ByteString
import com.datastax.driver.core.Row
import org.joda.time.DateTime

import io.mandelbrot.core.state.ProbeState
import io.mandelbrot.core.system._

trait PersisterStatements {

  val tableName: String

  def createStateTable =
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  probe_ref text PRIMARY KEY,
       |  generation bigint,
       |  timestamp timestamp,
       |  lifecycle text,
       |  health text,
       |  summary text,
       |  last_update timestamp,
       |  last_change timestamp,
       |  correlation uuid,
       |  acknowledged uuid,
       |  squelched boolean,
       |  context blob
       |)
     """.stripMargin

  def getStateStatement =
    s"""
       |SELECT probe_ref, generation, timestamp, lifecycle, health, summary, last_update, last_change, correlation, acknowledged, squelched, context
       |FROM $tableName
       |WHERE probe_ref = ?
     """.stripMargin

  def setStateStatement =
    s"""
       |INSERT INTO $tableName (probe_ref, generation, timestamp, lifecycle, health, summary, last_update, last_change, correlation, acknowledged, squelched, context)
       |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     """.stripMargin

  def deleteStateStatement =
    s"""
       |DELETE FROM $tableName WHERE probe_ref = ?
     """.stripMargin

  import scala.language.implicitConversions

  implicit def row2ProbeState(row: Row): ProbeState = {
    val probeRef = ProbeRef(row.getString(0))
    val generation = row.getLong(1)
    val timestamp = new DateTime(row.getDate(2))
    val lifecycle = row.getString(3) match {
      case "initializing" => ProbeInitializing
      case "joining" => ProbeJoining
      case "known" => ProbeKnown
      case "synthetic" => ProbeSynthetic
      case "retired" => ProbeRetired
    }
    val health = row.getString(4) match {
      case "healthy" => ProbeHealthy
      case "degraded" => ProbeDegraded
      case "failed" => ProbeFailed
      case "unknown" => ProbeUnknown
    }
    val summary = Option(row.getString(5))
    val lastUpdate = Option(row.getDate(6)).map(new DateTime(_))
    val lastChange = Option(row.getDate(7)).map(new DateTime(_))
    val correlation = Option(row.getUUID(8))
    val acknowledged = Option(row.getUUID(9))
    val squelched = row.getBool(10)
    val context = Option(row.getBytes(11)).map(ByteString(_))
    val status = ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlation, acknowledged, squelched)
    ProbeState(status, generation, None)
  }
}
