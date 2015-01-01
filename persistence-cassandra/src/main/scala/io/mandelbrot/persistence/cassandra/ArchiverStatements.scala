package io.mandelbrot.persistence.cassandra

import com.datastax.driver.core.Row
import org.joda.time.DateTime

import io.mandelbrot.core.system._
import io.mandelbrot.core.notification._

trait ArchiverStatements {

  val statusHistoryTableName: String
  val notificationHistoryTableName: String

  def createStatusHistoryTable =
    s"""
       |CREATE TABLE IF NOT EXISTS $statusHistoryTableName (
       |  probe_ref text,
       |  epoch bigint,
       |  timestamp timestamp,
       |  lifecycle text,
       |  health text,
       |  summary text,
       |  last_update timestamp,
       |  last_change timestamp,
       |  correlation uuid,
       |  acknowledged uuid,
       |  squelched boolean,
       |  PRIMARY KEY (probe_ref, epoch, timestamp)
       |)
     """.stripMargin


  def createNotificationHistoryTable =
    s"""
       |CREATE TABLE IF NOT EXISTS $notificationHistoryTableName (
       |  probe_ref text,
       |  epoch timestamp,
       |  timestamp timestamp,
       |  kind text,
       |  description text,
       |  correlation uuid,
       |  PRIMARY KEY (probe_ref, epoch, timestamp)
       |)
     """.stripMargin
  
  def insertStatusStatement =
    s"""
       |INSERT INTO $statusHistoryTableName (probe_ref, epoch, timestamp, lifecycle, health, summary, last_update, last_change, correlation, acknowledged, squelched)
       |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     """.stripMargin

  def insertNotificationStatement =
    s"""
       |INSERT INTO $notificationHistoryTableName (probe_ref, epoch, timestamp, kind, description, correlation)
       |VALUES (?, ?, ?, ?, ?, ?)
     """.stripMargin

  def cleanStatusHistoryStatement =
    s"""
       |DELETE FROM $statusHistoryTableName
       |WHERE probe_ref = ? AND epoch = ?
     """.stripMargin


  def cleanNotificationHistoryStatement =
    s"""
       |DELETE FROM $notificationHistoryTableName
       |WHERE probe_ref = ? AND epoch = ?
     """.stripMargin

  def getFirstStatusEpochStatement =
    s"""
       |SELECT epoch from $statusHistoryTableName
       |WHERE probe_ref = ?
       |ORDER BY epoch ASC
       |LIMIT 1
     """.stripMargin

  def getLastStatusEpochStatement =
    s"""
       |SELECT epoch from $statusHistoryTableName
       |WHERE probe_ref = ?
       |ORDER BY epoch DESC
       |LIMIT 1
     """.stripMargin

  def getStatusHistoryStatement =
    s"""
       |SELECT probe_ref, timestamp, lifecycle, health, summary, last_update, last_change, correlation, acknowledged, squelched
       |FROM $statusHistoryTableName
       |WHERE probe_ref = ? AND epoch = ? AND timestamp >= ?
       |LIMIT ?
     """.stripMargin

  def getFirstNotificationEpochStatement =
    s"""
       |SELECT epoch from $notificationHistoryTableName
       |WHERE probe_ref = ?
       |ORDER BY epoch ASC
       |LIMIT 1
     """.stripMargin

  def getLastNotificationEpochStatement =
    s"""
       |SELECT epoch from $notificationHistoryTableName
       |WHERE probe_ref = ?
       |ORDER BY epoch DESC
       |LIMIT 1
     """.stripMargin

  def getNotificationHistoryStatement =
    s"""
       |SELECT probe_ref, timestamp, kind, description, correlation
       |FROM $notificationHistoryTableName
       |WHERE probe_ref = ? AND epoch = ? AND timestamp >= ?
       |LIMIT ?
     """.stripMargin

  import scala.language.implicitConversions

  implicit def row2ProbeStatus(row: Row): ProbeStatus = {
    val probeRef = ProbeRef(row.getString(0))
    val timestamp = new DateTime(row.getDate(1))
    val lifecycle = row.getString(2) match {
      case "initializing" => ProbeInitializing
      case "joining" => ProbeJoining
      case "known" => ProbeKnown
      case "synthetic" => ProbeSynthetic
      case "retired" => ProbeRetired
    }
    val health = row.getString(3) match {
      case "healthy" => ProbeHealthy
      case "degraded" => ProbeDegraded
      case "failed" => ProbeFailed
      case "unknown" => ProbeUnknown
    }
    val summary = Option(row.getString(4))
    val lastUpdate = Option(row.getDate(5)).map(new DateTime(_))
    val lastChange = Option(row.getDate(6)).map(new DateTime(_))
    val correlation = Option(row.getUUID(7))
    val acknowledged = Option(row.getUUID(8))
    val squelched = row.getBool(9)
    ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlation, acknowledged, squelched)
  }

  implicit def row2ProbeNotification(row: Row): ProbeNotification = {
    val probeRef = ProbeRef(row.getString(0))
    val timestamp = new DateTime(row.getDate(2))
    val kind = row.getString(3)
    val description = row.getString(4)
    val correlation = Option(row.getUUID(5))
    ProbeNotification(probeRef, timestamp, kind, description, correlation)
  }
}
