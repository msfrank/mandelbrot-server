package io.mandelbrot.persistence.cassandra

import com.datastax.driver.core.{BoundStatement, Session}
import io.mandelbrot.core.notification.ProbeNotification
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConversions._
import org.joda.time.DateTime

import io.mandelbrot.core.{ApiException, ResourceNotFound}
import io.mandelbrot.core.state._
import io.mandelbrot.core.system._
import io.mandelbrot.persistence.cassandra.CassandraPersister.CassandraPersisterSettings

/**
 *
 */
class ProbeStatusDAL(settings: CassandraPersisterSettings,
                     val session: Session,
                     implicit val ec: ExecutionContext) extends AbstractDriver {
  import spray.json._

  val tableName: String = "probe_status"

  session.execute(
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  probe_ref text,
       |  epoch bigint,
       |  timestamp timestamp,
       |  last_update timestamp,
       |  last_change timestamp,
       |  condition text,
       |  notifications text,
       |  metrics text,
       |  PRIMARY KEY ((probe_ref, epoch), timestamp)
       |)
     """.stripMargin)

  private val preparedUpdateProbeStatus = session.prepare(
    s"""
       |INSERT INTO $tableName (probe_ref, epoch, timestamp, last_update, last_change, condition, notifications, metrics)
       |VALUES (?, ?, ?, ?, ?, ?, ?, ?)
     """.stripMargin)

  def updateProbeStatus(probeRef: ProbeRef, epoch: Long, probeStatus: ProbeStatus, notifications: Vector[ProbeNotification]): Future[Unit] = {
    val _probeRef = probeRef.toString
    val _epoch: java.lang.Long = epoch
    val timestamp = probeStatus.timestamp.toDate
    val lastUpdate = probeStatus.lastUpdate.map(_.toDate).orNull
    val lastChange = probeStatus.lastChange.map(_.toDate).orNull
    val condition = ProbeCondition(probeStatus.timestamp, probeStatus.lifecycle, probeStatus.summary,
      probeStatus.health, probeStatus.correlation, probeStatus.acknowledged, probeStatus.squelched)
    val _condition = probeCondition2string(condition)
    val _notifications = if (notifications.nonEmpty) probeNotifications2string(ProbeNotifications(notifications)) else null
    val _metrics = if (probeStatus.metrics.nonEmpty) probeMetrics2string(ProbeMetrics(probeStatus.metrics)) else null
    val statement = new BoundStatement(preparedUpdateProbeStatus)
    statement.bind(_probeRef, _epoch, timestamp, lastUpdate, lastChange, _condition, _notifications, _metrics)
    executeAsync(statement).map { _ => Unit }
  }

  private val preparedGetProbeStatus = session.prepare(
    s"""
       |SELECT timestamp, last_update, last_change, condition, metrics
       |FROM $tableName
       |WHERE probe_ref = ? AND epoch = ? AND timestamp = ?
     """.stripMargin)

  def getProbeStatus(probeRef: ProbeRef, epoch: Long, timestamp: DateTime): Future[ProbeStatus] = {
    val _probeRef = probeRef.toString
    val _epoch: java.lang.Long = epoch
    val _timestamp = timestamp.toDate
    val statement = new BoundStatement(preparedGetProbeStatus)
    statement.bind(_probeRef, _epoch, _timestamp)
    executeAsync(statement).map {
      case resultSet =>
        val row = resultSet.one()
        if (row == null) throw ApiException(ResourceNotFound) else {
          val timestamp = new DateTime(row.getDate(0))
          val lastUpdate = Option(row.getDate(1)).map(new DateTime(_))
          val lastChange = Option(row.getDate(2)).map(new DateTime(_))
          val condition = string2probeCondition(row.getString(3))
          val metrics = Option(string2probeMetrics(row.getString(4)))
            .map(_.metrics)
            .getOrElse(Map.empty[String,BigDecimal])
          ProbeStatus(timestamp, condition.lifecycle, condition.summary, condition.health, metrics,
            lastUpdate, lastChange, condition.correlation, condition.acknowledged, condition.squelched)
        }
    }
  }

  private val preparedGetProbeCondition = session.prepare(
    s"""
       |SELECT condition
       |FROM $tableName
       |WHERE probe_ref = ? AND epoch = ? AND timestamp = ?
     """.stripMargin)

  def getProbeCondition(probeRef: ProbeRef, epoch: Long, timestamp: DateTime): Future[ProbeCondition] = {
    val _probeRef = probeRef.toString
    val _epoch: java.lang.Long = epoch
    val _timestamp = timestamp.toDate
    val statement = new BoundStatement(preparedGetProbeCondition)
    statement.bind(_probeRef, _epoch, _timestamp)
    executeAsync(statement).map {
      case resultSet =>
        val row = resultSet.one()
        if (row == null) throw ApiException(ResourceNotFound) else string2probeCondition(row.getString(0))
    }
  }

  private val preparedGetProbeConditionHistory = session.prepare(
    s"""
       |SELECT condition
       |FROM $tableName
       |WHERE probe_ref = ? AND epoch = ? AND timestamp >= ? AND timestamp < ?
       |LIMIT ?
     """.stripMargin)

  def getProbeConditionHistory(probeRef: ProbeRef, epoch: Long, from: Option[DateTime], to: Option[DateTime], limit: Int): Future[Vector[ProbeCondition]] = {
    val _probeRef = probeRef.toString
    val _epoch: java.lang.Long = epoch
    val start = from.map(_.toDate).getOrElse(EpochUtils.SMALLEST_DATE)
    val end = to.map(_.toDate).getOrElse(EpochUtils.LARGEST_DATE)
    val _limit: java.lang.Integer = limit
    val statement = new BoundStatement(preparedGetProbeConditionHistory)
    statement.bind(_probeRef, _epoch, start, end, _limit)
    executeAsync(statement).map { resultSet =>
      resultSet.all().map(row => string2probeCondition(row.getString(0))).toVector
    }
  }

  private val preparedGetProbeNotifications = session.prepare(
    s"""
       |SELECT notifications
       |FROM $tableName
       |WHERE probe_ref = ? AND epoch = ? AND timestamp = ?
     """.stripMargin)

  def getProbeNotifications(probeRef: ProbeRef, epoch: Long, timestamp: DateTime): Future[ProbeNotifications] = {
    val _probeRef = probeRef.toString
    val _epoch: java.lang.Long = epoch
    val _timestamp = timestamp.toDate
    val statement = new BoundStatement(preparedGetProbeNotifications)
    statement.bind(_probeRef, _epoch, _timestamp)
    executeAsync(statement).map {
      case resultSet =>
        val row = resultSet.one()
        if (row == null) throw ApiException(ResourceNotFound) else string2probeNotifications(row.getString(0))
    }
  }

  private val preparedGetProbeNotificationsHistory = session.prepare(
    s"""
       |SELECT notifications
       |FROM $tableName
       |WHERE probe_ref = ? AND epoch = ? AND timestamp >= ? AND timestamp < ?
       |LIMIT ?
     """.stripMargin)

  def getProbeNotificationsHistory(probeRef: ProbeRef, epoch: Long, from: Option[DateTime], to: Option[DateTime], limit: Int): Future[Vector[ProbeNotifications]] = {
    val _probeRef = probeRef.toString
    val _epoch: java.lang.Long = epoch
    val start = from.map(_.toDate).getOrElse(EpochUtils.SMALLEST_DATE)
    val end = to.map(_.toDate).getOrElse(EpochUtils.LARGEST_DATE)
    val _limit: java.lang.Integer = limit
    val statement = new BoundStatement(preparedGetProbeNotificationsHistory)
    statement.bind(_probeRef, _epoch, start, end, _limit)
    executeAsync(statement).map { resultSet =>
      resultSet.all().map(row => string2probeNotifications(row.getString(0))).toVector
    }
  }

  private val preparedGetProbeMetrics = session.prepare(
    s"""
       |SELECT metrics
       |FROM $tableName
       |WHERE probe_ref = ? AND epoch = ? AND timestamp = ?
     """.stripMargin)

  def getProbeMetrics(probeRef: ProbeRef, epoch: Long, timestamp: DateTime): Future[ProbeMetrics] = {
    val _probeRef = probeRef.toString
    val _epoch: java.lang.Long = epoch
    val _timestamp = timestamp.toDate
    val statement = new BoundStatement(preparedGetProbeMetrics)
    statement.bind(_probeRef, _epoch, _timestamp)
    executeAsync(statement).map {
      case resultSet =>
        val row = resultSet.one()
        if (row == null) throw ApiException(ResourceNotFound) else string2probeMetrics(row.getString(0))
    }
  }

  private val preparedGetProbeMetricsHistory = session.prepare(
    s"""
       |SELECT metrics
       |FROM $tableName
       |WHERE probe_ref = ? AND epoch = ? AND timestamp >= ? AND timestamp < ?
       |LIMIT ?
     """.stripMargin)

  def getProbeMetricsHistory(probeRef: ProbeRef, epoch: Long, from: Option[DateTime], to: Option[DateTime], limit: Int): Future[Vector[ProbeMetrics]] = {
    val _probeRef = probeRef.toString
    val _epoch: java.lang.Long = epoch
    val start = from.map(_.toDate).getOrElse(EpochUtils.SMALLEST_DATE)
    val end = to.map(_.toDate).getOrElse(EpochUtils.LARGEST_DATE)
    val _limit: java.lang.Integer = limit
    val statement = new BoundStatement(preparedGetProbeMetricsHistory)
    statement.bind(_probeRef, _epoch, start, end, _limit)
    executeAsync(statement).map { resultSet =>
      resultSet.all().map(row => string2probeMetrics(row.getString(0))).toVector
    }
  }

  private val preparedDeleteProbeStatus = session.prepare(
    s"""
       |DELETE FROM $tableName WHERE probe_ref = ? AND epoch = ?
     """.stripMargin)

  def deleteProbeStatus(probeRef: ProbeRef, epoch: Long): Future[Unit] = {
    val _probeRef = probeRef.toString
    val _epoch: java.lang.Long = epoch
    val statement = new BoundStatement(preparedDeleteProbeStatus)
    statement.bind(_probeRef, _epoch)
    executeAsync(statement).map { _ => Unit }
  }

  def flushProbeStatus(): Future[Unit] = {
    executeAsync(s"TRUNCATE $tableName").map { _ => Unit }
  }

  import io.mandelbrot.core.http.JsonProtocol._

  def string2probeCondition(string: String): ProbeCondition = string.parseJson.convertTo[ProbeCondition]

  def probeCondition2string(condition: ProbeCondition): String = condition.toJson.prettyPrint

  def string2probeNotifications(string: String): ProbeNotifications = string.parseJson.convertTo[ProbeNotifications]

  def probeNotifications2string(notifications: ProbeNotifications): String = notifications.toJson.prettyPrint

  def string2probeMetrics(string: String): ProbeMetrics = string.parseJson.convertTo[ProbeMetrics]

  def probeMetrics2string(metrics: ProbeMetrics): String = metrics.toJson.prettyPrint
}
