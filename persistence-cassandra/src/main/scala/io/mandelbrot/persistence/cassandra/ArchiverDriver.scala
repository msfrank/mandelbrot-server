package io.mandelbrot.persistence.cassandra

import com.datastax.driver.core.{BoundStatement, Row, Session}
import org.joda.time.DateTime
import scala.concurrent.{Future, ExecutionContext}
import scala.collection.JavaConversions._
import java.util.Date

import io.mandelbrot.core.notification.ProbeNotification
import io.mandelbrot.core.system._
import io.mandelbrot.persistence.cassandra.CassandraArchiver.CassandraArchiverSettings

/**
 *
 */
class ArchiverDriver(settings: CassandraArchiverSettings, session: Session)(implicit ec: ExecutionContext) extends AbstractDriver(session, ec) {


  val statusHistoryTableName = "history_s"
  val notificationHistoryTableName = "history_n"
  val metricsHistoryTableName = "history_m"

  val LARGEST_DATE = new Date(java.lang.Long.MAX_VALUE)
  val SMALLEST_DATE = new Date(0)

  session.execute(
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
     """.stripMargin)

  session.execute(
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
     """.stripMargin)

  val preparedInsertStatus = session.prepare(
    s"""
       |INSERT INTO $statusHistoryTableName (probe_ref, epoch, timestamp, lifecycle, health, summary, last_update, last_change, correlation, acknowledged, squelched)
       |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     """.stripMargin)

  def insertStatus(status: ProbeStatus, epoch: Long): Future[ProbeStatus] = {
    val probeRef = status.probeRef.toString
    val _epoch: java.lang.Long = epoch
    val timestamp = status.timestamp.toDate
    val lifecycle = status.lifecycle.toString
    val health = status.health.toString
    val summary = status.summary.orNull
    val lastUpdate = status.lastUpdate.map(_.toDate).orNull
    val lastChange = status.lastChange.map(_.toDate).orNull
    val correlation = status.correlation.orNull
    val acknowledged = status.acknowledged.orNull
    val squelched: java.lang.Boolean = status.squelched
    session.executeAsync(new BoundStatement(preparedInsertStatus).bind(probeRef, _epoch,
      timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlation, acknowledged,
      squelched)).map { _ => status }
  }

  val preparedInsertNotification = session.prepare(
    s"""
       |INSERT INTO $notificationHistoryTableName (probe_ref, epoch, timestamp, kind, description, correlation)
       |VALUES (?, ?, ?, ?, ?, ?)
     """.stripMargin)

  def insertNotification(notification: ProbeNotification, epoch: Long): Future[ProbeNotification] = {
    val probeRef = notification.probeRef.toString
    val _epoch: java.lang.Long = epoch
    val timestamp = notification.timestamp.toDate
    val kind = notification.kind
    val description = notification.description
    val correlation = notification.correlation.orNull
    session.executeAsync(new BoundStatement(preparedInsertNotification).bind(probeRef, _epoch,
      timestamp, kind, description, correlation)).map { _ => notification }
  }

  val preparedCleanStatusHistory = session.prepare(
    s"""
       |DELETE FROM $statusHistoryTableName
       |WHERE probe_ref = ? AND epoch = ?
     """.stripMargin)

  def cleanStatusHistory(probeRef: ProbeRef, epoch: Long): Future[Unit] = {
    session.executeAsync(new BoundStatement(preparedCleanStatusHistory).bind(probeRef.toString,
      epoch: java.lang.Long)).map { _ => Unit }
  }

  val preparedCleanNotificationHistory = session.prepare(
    s"""
       |DELETE FROM $notificationHistoryTableName
       |WHERE probe_ref = ? AND epoch = ?
     """.stripMargin)

  def cleanNotificationHistory(probeRef: ProbeRef, epoch: Long): Future[Unit] = {
    session.executeAsync(new BoundStatement(preparedCleanNotificationHistory).bind(probeRef.toString,
      epoch: java.lang.Long)).map { _ => Unit }
  }

  val preparedGetFirstStatusEpoch = session.prepare(
    s"""
       |SELECT epoch from $statusHistoryTableName
       |WHERE probe_ref = ?
       |ORDER BY epoch ASC
       |LIMIT 1
     """.stripMargin)

  def getFirstStatusEpoch(probeRef: ProbeRef): Future[Option[Long]] = {
    session.executeAsync(new BoundStatement(preparedGetFirstStatusEpoch).bind(probeRef.toString)).map { resultSet =>
      val row = resultSet.one()
      if (row != null) Some(row.getLong(0)) else None
    }
  }

  val preparedGetLastStatusEpoch = session.prepare(
    s"""
       |SELECT epoch from $statusHistoryTableName
       |WHERE probe_ref = ?
       |ORDER BY epoch DESC
       |LIMIT 1
     """.stripMargin)

  def getLastStatusEpoch(probeRef: ProbeRef): Future[Option[Long]] = {
    session.executeAsync(new BoundStatement(preparedGetLastStatusEpoch).bind(probeRef.toString)).map { resultSet =>
      val row = resultSet.one()
      if (row != null) Some(row.getLong(0)) else None
    }
  }

  val preparedGetStatusHistory = session.prepare(
    s"""
       |SELECT probe_ref, timestamp, lifecycle, health, summary, last_update, last_change, correlation, acknowledged, squelched
       |FROM $statusHistoryTableName
       |WHERE probe_ref = ? AND epoch = ? AND timestamp >= ? AND timestamp < ?
       |LIMIT ?
     """.stripMargin)

  def getStatusHistory(probeRef: ProbeRef, epoch: Long, from: Option[DateTime], to: Option[DateTime], limit: Int): Future[Vector[ProbeStatus]] = {
    val _epoch: java.lang.Long = epoch
    val start = from.map(_.toDate).getOrElse(SMALLEST_DATE)
    val end = to.map(_.toDate).getOrElse(LARGEST_DATE)
    val _limit: java.lang.Integer = limit
    session.executeAsync(new BoundStatement(preparedGetStatusHistory).bind(probeRef.toString, _epoch, start, end, _limit)).map { resultSet =>
      resultSet.all().map(row2ProbeStatus).toVector
    }
  }

  val preparedGetFirstNotificationEpoch = session.prepare(
    s"""
       |SELECT epoch from $notificationHistoryTableName
       |WHERE probe_ref = ?
       |ORDER BY epoch ASC
       |LIMIT 1
     """.stripMargin)

  def getFirstNotificationEpoch(probeRef: ProbeRef): Future[Option[Long]] = {
    session.executeAsync(new BoundStatement(preparedGetFirstNotificationEpoch).bind(probeRef.toString)).map { resultSet =>
      val row = resultSet.one()
      if (row != null) Some(row.getLong(0)) else None
    }
  }

  val preparedGetLastNotificationEpoch = session.prepare(
    s"""
       |SELECT epoch from $notificationHistoryTableName
       |WHERE probe_ref = ?
       |ORDER BY epoch DESC
       |LIMIT 1
     """.stripMargin)

  def getLastNotificationEpoch(probeRef: ProbeRef): Future[Option[Long]] = {
    session.executeAsync(new BoundStatement(preparedGetLastNotificationEpoch).bind(probeRef.toString)).map { resultSet =>
      val row = resultSet.one()
      if (row != null) Some(row.getLong(0)) else None
    }
  }

  val preparedGetNotificationHistory = session.prepare(
    s"""
       |SELECT probe_ref, timestamp, kind, description, correlation
       |FROM $notificationHistoryTableName
       |WHERE probe_ref = ? AND epoch = ? AND timestamp >= ? AND timestamp < ?
       |LIMIT ?
     """.stripMargin)

  def getNotificationHistory(probeRef: ProbeRef, epoch: Long, from: Option[DateTime], to: Option[DateTime], limit: Int): Future[Vector[ProbeNotification]] = {
    val _epoch: java.lang.Long = epoch
    val start = from.map(_.toDate).getOrElse(SMALLEST_DATE)
    val end = to.map(_.toDate).getOrElse(LARGEST_DATE)
    val _limit: java.lang.Integer = limit
    session.executeAsync(new BoundStatement(preparedGetNotificationHistory).bind(probeRef.toString, _epoch, start, end, _limit)).map { resultSet =>
      resultSet.all().map(row2ProbeNotification).toVector
    }
  }

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
