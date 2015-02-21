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
class NotificationHistoryDAL(settings: CassandraArchiverSettings, session: Session)(implicit ec: ExecutionContext) extends AbstractDriver(session, ec) {

  val tableName = "history_n"

  val LARGEST_DATE = new Date(java.lang.Long.MAX_VALUE)
  val SMALLEST_DATE = new Date(0)

  session.execute(
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  probe_ref text,
       |  epoch timestamp,
       |  timestamp timestamp,
       |  kind text,
       |  description text,
       |  correlation uuid,
       |  PRIMARY KEY (probe_ref, epoch, timestamp)
       |)
     """.stripMargin)

  private val preparedInsertNotification = session.prepare(
    s"""
       |INSERT INTO $tableName (probe_ref, epoch, timestamp, kind, description, correlation)
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

  private val preparedCleanNotificationHistory = session.prepare(
    s"""
       |DELETE FROM $tableName
       |WHERE probe_ref = ? AND epoch = ?
     """.stripMargin)

  def cleanNotificationHistory(probeRef: ProbeRef, epoch: Long): Future[Unit] = {
    session.executeAsync(new BoundStatement(preparedCleanNotificationHistory).bind(probeRef.toString,
      epoch: java.lang.Long)).map { _ => Unit }
  }

  private val preparedGetFirstNotificationEpoch = session.prepare(
    s"""
       |SELECT epoch from $tableName
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

  private val preparedGetLastNotificationEpoch = session.prepare(
    s"""
       |SELECT epoch from $tableName
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

  private val preparedGetNotificationHistory = session.prepare(
    s"""
       |SELECT probe_ref, timestamp, kind, description, correlation
       |FROM $tableName
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

  implicit def row2ProbeNotification(row: Row): ProbeNotification = {
    val probeRef = ProbeRef(row.getString(0))
    val timestamp = new DateTime(row.getDate(2))
    val kind = row.getString(3)
    val description = row.getString(4)
    val correlation = Option(row.getUUID(5))
    ProbeNotification(probeRef, timestamp, kind, description, correlation)
  }
}
