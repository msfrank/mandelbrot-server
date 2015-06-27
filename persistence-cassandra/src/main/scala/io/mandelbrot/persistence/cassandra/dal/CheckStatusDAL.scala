package io.mandelbrot.persistence.cassandra.dal

import com.datastax.driver.core.{BoundStatement, Session}
import com.datastax.driver.core.querybuilder.{Clause, QueryBuilder}
import spray.json._
import org.joda.time.{DateTime, DateTimeZone}
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConversions._

import io.mandelbrot.core.http.json.JsonProtocol._
import io.mandelbrot.core.model._
import io.mandelbrot.core.{ApiException, ResourceNotFound}
import io.mandelbrot.persistence.cassandra.{EpochUtils, CassandraStatePersisterSettings}

/**
 *
 */
class CheckStatusDAL(settings: CassandraStatePersisterSettings,
                     val session: Session,
                     implicit val ec: ExecutionContext) extends AbstractDriver {


  val tableName: String = "check_status"

  session.execute(
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  check_ref text,
       |  generation bigint,
       |  epoch bigint,
       |  timestamp timestamp,
       |  last_update timestamp,
       |  last_change timestamp,
       |  condition text,
       |  notifications text,
       |  metrics text,
       |  PRIMARY KEY ((check_ref, generation, epoch), timestamp)
       |)
     """.stripMargin)

  private val preparedUpdateCheckStatus = session.prepare(
    s"""
       |INSERT INTO $tableName (check_ref, generation, epoch, timestamp, last_update, last_change, condition, notifications, metrics)
       |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
     """.stripMargin)

  def updateCheckStatus(checkRef: CheckRef, generation: Long, epoch: Long, checkStatus: CheckStatus, notifications: Vector[CheckNotification]): Future[Unit] = {
    val _checkRef = checkRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val timestamp = checkStatus.timestamp.toDate
    val lastUpdate = checkStatus.lastUpdate.map(_.toDate).orNull
    val lastChange = checkStatus.lastChange.map(_.toDate).orNull
    val condition = CheckCondition(generation, checkStatus.timestamp, checkStatus.lifecycle, checkStatus.summary,
      checkStatus.health, checkStatus.correlation, checkStatus.acknowledged, checkStatus.squelched)
    val _condition = checkCondition2string(condition)
    val _notifications = if (notifications.nonEmpty) checkNotifications2string(CheckNotifications(generation, checkStatus.timestamp, notifications)) else null
    val _metrics = if (checkStatus.metrics.nonEmpty) checkMetrics2string(CheckMetrics(generation, checkStatus.timestamp, checkStatus.metrics)) else null
    val statement = new BoundStatement(preparedUpdateCheckStatus)
    statement.bind(_checkRef, _generation, _epoch, timestamp, lastUpdate, lastChange, _condition, _notifications, _metrics)
    executeAsync(statement).map { _ => Unit }
  }

  private val preparedCheckIfEpochExhausted = session.prepare(
    s"""
       |SELECT timestamp
       |FROM $tableName
       |WHERE check_ref = ? AND generation = ? AND epoch = ? AND timestamp > ?
       |LIMIT 1
     """.stripMargin)

  def checkIfEpochExhausted(checkRef: CheckRef, generation: Long, epoch: Long, timestamp: DateTime): Future[Boolean] = {
    val _checkRef = checkRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val _timestamp = timestamp.toDate
    val statement = new BoundStatement(preparedCheckIfEpochExhausted)
    statement.bind(_checkRef, _generation, _epoch, _timestamp)
    executeAsync(statement).map {
      case resultSet => resultSet.isFullyFetched && resultSet.isExhausted
    }
  }

  private val preparedGetCheckStatus = session.prepare(
    s"""
       |SELECT timestamp, last_update, last_change, condition, metrics
       |FROM $tableName
       |WHERE check_ref = ? AND generation = ? AND epoch = ? AND timestamp = ?
     """.stripMargin)

  def getCheckStatus(checkRef: CheckRef, generation: Long, epoch: Long, timestamp: DateTime): Future[CheckStatus] = {
    val _checkRef = checkRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val _timestamp = timestamp.toDate
    val statement = new BoundStatement(preparedGetCheckStatus)
    statement.bind(_checkRef, _generation, _epoch, _timestamp)
    executeAsync(statement).map {
      case resultSet =>
        val row = resultSet.one()
        if (row == null) throw ApiException(ResourceNotFound) else {
          val timestamp = new DateTime(row.getDate(0), DateTimeZone.UTC)
          val lastUpdate = Option(row.getDate(1)).map(new DateTime(_, DateTimeZone.UTC))
          val lastChange = Option(row.getDate(2)).map(new DateTime(_, DateTimeZone.UTC))
          val condition = string2checkCondition(row.getString(3))
          val metrics = Option(row.getString(4))
            .map(string2checkMetrics)
            .map(_.metrics)
            .getOrElse(Map.empty[String,BigDecimal])
          CheckStatus(generation, timestamp, condition.lifecycle, condition.summary, condition.health, metrics,
            lastUpdate, lastChange, condition.correlation, condition.acknowledged, condition.squelched)
        }
    }
  }

  /**
   *
   */
  def startClause(from: Option[DateTime], fromExclusive: Boolean): Clause = from match {
    case None if fromExclusive => QueryBuilder.gt("timestamp", EpochUtils.SMALLEST_DATE)
    case None => QueryBuilder.gte("timestamp", EpochUtils.SMALLEST_DATE)
    case Some(timestamp) if fromExclusive => QueryBuilder.gt("timestamp", timestamp.toDate)
    case Some(timestamp) => QueryBuilder.gte("timestamp", timestamp.toDate)
  }

  /**
   *
   */
  def endClause(to: Option[DateTime], toInclusive: Boolean): Clause = to match {
    case None if toInclusive => QueryBuilder.lte("timestamp", EpochUtils.LARGEST_DATE)
    case None => QueryBuilder.lt("timestamp", EpochUtils.LARGEST_DATE)
    case Some(timestamp) if toInclusive => QueryBuilder.lte("timestamp", timestamp.toDate)
    case Some(timestamp) => QueryBuilder.lt("timestamp", timestamp.toDate)
  }

  private val preparedGetCheckCondition = session.prepare(
    s"""
       |SELECT condition
       |FROM $tableName
       |WHERE check_ref = ? AND generation = ? AND epoch = ? AND timestamp = ?
     """.stripMargin)

  def getCheckCondition(checkRef: CheckRef,
                        generation: Long,
                        epoch: Long,
                        timestamp: DateTime): Future[CheckCondition] = {
    val _checkRef = checkRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val _timestamp = timestamp.toDate
    val statement = new BoundStatement(preparedGetCheckCondition)
    statement.bind(_checkRef, _generation, _epoch, _timestamp)
    executeAsync(statement).map {
      case resultSet =>
        val row = resultSet.one()
        if (row == null) throw ApiException(ResourceNotFound) else string2checkCondition(row.getString(0))
    }
  }

  private val preparedGetCheckConditionHistory = session.prepare(
    s"""
       |SELECT condition
       |FROM $tableName
       |WHERE check_ref = ? AND generation = ? AND epoch = ? AND timestamp >= ? AND timestamp < ?
       |LIMIT ?
     """.stripMargin)

  def getCheckConditionHistory(checkRef: CheckRef,
                               generation: Long,
                               epoch: Long,
                               from: Option[DateTime],
                               to: Option[DateTime],
                               limit: Int): Future[CheckConditionHistory] = {
    val _checkRef = checkRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val start = from.map(_.toDate).getOrElse(EpochUtils.SMALLEST_DATE)
    val end = to.map(_.toDate).getOrElse(EpochUtils.LARGEST_DATE)
    val _limit: java.lang.Integer = limit
    val statement = new BoundStatement(preparedGetCheckConditionHistory)
    statement.bind(_checkRef, _generation, _epoch, start, end, _limit)
    statement.setFetchSize(limit)
    executeAsync(statement).map { resultSet =>
      val conditions = resultSet.all()
        .map(row => string2checkCondition(row.getString(0)))
        .toVector
      CheckConditionHistory(conditions)
    }
  }

  /**
   *
   */
  def getCheckConditionHistory(checkRef: CheckRef,
                               generation: Long,
                               epoch: Long,
                               from: Option[DateTime],
                               to: Option[DateTime],
                               limit: Int,
                               fromExclusive: Boolean,
                               toInclusive: Boolean,
                               descending: Boolean): Future[CheckConditionHistory] = {
    val start = startClause(from, fromExclusive)
    val end = endClause(to, toInclusive)
    val ordering = if (descending) QueryBuilder.desc("timestamp") else QueryBuilder.asc("timestamp")
    val select = QueryBuilder.select("condition")
      .from(tableName)
      .where(QueryBuilder.eq("check_ref", checkRef.toString))
        .and(QueryBuilder.eq("generation", generation: java.lang.Long))
        .and(QueryBuilder.eq("epoch", epoch))
        .and(start)
        .and(end)
      .orderBy(ordering)
      .limit(limit)
    select.setFetchSize(limit)
    executeAsync(select).map { resultSet =>
      val conditions = resultSet.all()
        .map(row => string2checkCondition(row.getString(0)))
        .toVector
      CheckConditionHistory(conditions)
    }
  }

  private val preparedGetCheckNotifications = session.prepare(
    s"""
       |SELECT notifications
       |FROM $tableName
       |WHERE check_ref = ? AND generation = ? AND epoch = ? AND timestamp = ?
     """.stripMargin)

  def getCheckNotifications(checkRef: CheckRef,
                            generation: Long,
                            epoch: Long,
                            timestamp: DateTime): Future[CheckNotifications] = {
    val _checkRef = checkRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val _timestamp = timestamp.toDate
    val statement = new BoundStatement(preparedGetCheckNotifications)
    statement.bind(_checkRef, _generation, _epoch, _timestamp)
    executeAsync(statement).map {
      case resultSet =>
        val row = resultSet.one()
        if (row == null) throw ApiException(ResourceNotFound) else {
          Option(row.getString(0))
            .map(string2checkNotifications)
            .getOrElse(CheckNotifications(generation, timestamp, Vector.empty))
        }
    }
  }

  private val preparedGetCheckNotificationsHistory = session.prepare(
    s"""
       |SELECT notifications
       |FROM $tableName
       |WHERE check_ref = ? AND generation = ? AND epoch = ? AND timestamp >= ? AND timestamp < ?
       |LIMIT ?
     """.stripMargin)

  def getCheckNotificationsHistory(checkRef: CheckRef,
                                   generation: Long,
                                   epoch: Long,
                                   from: Option[DateTime],
                                   to: Option[DateTime],
                                   limit: Int): Future[CheckNotificationsHistory] = {
    val _checkRef = checkRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val start = from.map(_.toDate).getOrElse(EpochUtils.SMALLEST_DATE)
    val end = to.map(_.toDate).getOrElse(EpochUtils.LARGEST_DATE)
    val _limit: java.lang.Integer = limit
    val statement = new BoundStatement(preparedGetCheckNotificationsHistory)
    statement.bind(_checkRef, _generation, _epoch, start, end, _limit)
    statement.setFetchSize(limit)
    executeAsync(statement).map { resultSet =>
      val notifications = resultSet.all()
        .flatMap(row => Option(row.getString(0)))
        .map(string2checkNotifications)
        .toVector
      CheckNotificationsHistory(notifications)
    }
  }

  /**
   *
   */
  def getCheckNotificationsHistory(checkRef: CheckRef,
                                   generation: Long,
                                   epoch: Long,
                                   from: Option[DateTime],
                                   to: Option[DateTime],
                                   limit: Int,
                                   fromExclusive: Boolean,
                                   toInclusive: Boolean,
                                   descending: Boolean): Future[CheckNotificationsHistory] = {
    val start = startClause(from, fromExclusive)
    val end = endClause(to, toInclusive)
    val ordering = if (descending) QueryBuilder.desc("timestamp") else QueryBuilder.asc("timestamp")
    val select = QueryBuilder.select("notifications")
      .from(tableName)
      .where(QueryBuilder.eq("check_ref", checkRef.toString))
        .and(QueryBuilder.eq("generation", generation: java.lang.Long))
        .and(QueryBuilder.eq("epoch", epoch))
        .and(start)
        .and(end)
      .orderBy(ordering)
      .limit(limit)
    select.setFetchSize(limit)
    executeAsync(select).map { resultSet =>
      val notifications = resultSet.all()
        .map(row => string2checkNotifications(row.getString(0)))
        .toVector
      CheckNotificationsHistory(notifications)
    }
  }

  private val preparedGetCheckMetrics = session.prepare(
    s"""
       |SELECT metrics
       |FROM $tableName
       |WHERE check_ref = ? AND generation = ? AND epoch = ? AND timestamp = ?
     """.stripMargin)

  def getCheckMetrics(checkRef: CheckRef,
                      generation: Long,
                      epoch: Long,
                      timestamp: DateTime): Future[CheckMetrics] = {
    val _checkRef = checkRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val _timestamp = timestamp.toDate
    val statement = new BoundStatement(preparedGetCheckMetrics)
    statement.bind(_checkRef, _generation, _epoch, _timestamp)
    executeAsync(statement).map {
      case resultSet =>
        val row = resultSet.one()
        if (row == null) throw ApiException(ResourceNotFound) else {
          Option(row.getString(0))
            .map(string2checkMetrics)
            .getOrElse(CheckMetrics(generation, timestamp, Map.empty))
        }
    }
  }

  private val preparedGetCheckMetricsHistory = session.prepare(
    s"""
       |SELECT metrics
       |FROM $tableName
       |WHERE check_ref = ? AND generation = ? AND epoch = ? AND timestamp >= ? AND timestamp < ?
       |LIMIT ?
     """.stripMargin)

  def getCheckMetricsHistory(checkRef: CheckRef,
                             generation: Long,
                             epoch: Long,
                             from: Option[DateTime],
                             to: Option[DateTime],
                             limit: Int): Future[CheckMetricsHistory] = {
    val _checkRef = checkRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val start = from.map(_.toDate).getOrElse(EpochUtils.SMALLEST_DATE)
    val end = to.map(_.toDate).getOrElse(EpochUtils.LARGEST_DATE)
    val _limit: java.lang.Integer = limit
    val statement = new BoundStatement(preparedGetCheckMetricsHistory)
    statement.bind(_checkRef, _generation, _epoch, start, end, _limit)
    statement.setFetchSize(limit)
    executeAsync(statement).map { resultSet =>
      val metrics = resultSet.all()
        .flatMap(row => Option(row.getString(0)))
        .map(string2checkMetrics)
        .toVector
      CheckMetricsHistory(metrics)
    }
  }

  /**
   *
   */
  def getCheckMetricsHistory(checkRef: CheckRef,
                             generation: Long,
                             epoch: Long,
                             from: Option[DateTime],
                             to: Option[DateTime],
                             limit: Int,
                             fromExclusive: Boolean,
                             toInclusive: Boolean,
                             descending: Boolean): Future[CheckMetricsHistory] = {
    val start = startClause(from, fromExclusive)
    val end = endClause(to, toInclusive)
    val ordering = if (descending) QueryBuilder.desc("timestamp") else QueryBuilder.asc("timestamp")
    val select = QueryBuilder.select("metrics")
      .from(tableName)
      .where(QueryBuilder.eq("check_ref", checkRef.toString))
        .and(QueryBuilder.eq("generation", generation: java.lang.Long))
        .and(QueryBuilder.eq("epoch", epoch))
        .and(start)
        .and(end)
      .orderBy(ordering)
      .limit(limit)
    select.setFetchSize(limit)
    executeAsync(select).map { resultSet =>
      val metrics = resultSet.all()
        .map(row => string2checkMetrics(row.getString(0)))
        .toVector
      CheckMetricsHistory(metrics)
    }
  }

  private val preparedGetFirstCheckStatus = session.prepare(
    s"""
       |SELECT timestamp, last_update, last_change, condition, metrics
       |FROM $tableName
       |WHERE check_ref = ? AND generation = ? AND epoch = ?
       |ORDER BY timestamp ASC
       |LIMIT 1
     """.stripMargin)

  def getFirstCheckStatus(checkRef: CheckRef, generation: Long, epoch: Long): Future[CheckStatus] = {
    val _checkRef = checkRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val statement = new BoundStatement(preparedGetFirstCheckStatus)
    statement.bind(_checkRef, _generation, _epoch)
    executeAsync(statement).map {
      case resultSet =>
        val row = resultSet.one()
        if (row == null) throw ApiException(ResourceNotFound) else {
          val timestamp = new DateTime(row.getDate(0), DateTimeZone.UTC)
          val lastUpdate = Option(row.getDate(1)).map(new DateTime(_, DateTimeZone.UTC))
          val lastChange = Option(row.getDate(2)).map(new DateTime(_, DateTimeZone.UTC))
          val condition = string2checkCondition(row.getString(3))
          val metrics = Option(row.getString(4))
            .map(string2checkMetrics)
            .map(_.metrics)
            .getOrElse(Map.empty[String,BigDecimal])
          CheckStatus(generation, timestamp, condition.lifecycle, condition.summary, condition.health, metrics,
            lastUpdate, lastChange, condition.correlation, condition.acknowledged, condition.squelched)
        }
    }
  }

  private val preparedGetLastCheckStatus = session.prepare(
    s"""
       |SELECT timestamp, last_update, last_change, condition, metrics
       |FROM $tableName
       |WHERE check_ref = ? AND generation = ? AND epoch = ?
       |ORDER BY timestamp DESC
       |LIMIT 1
     """.stripMargin)

  def getLastCheckStatus(checkRef: CheckRef, generation: Long, epoch: Long): Future[CheckStatus] = {
    val _checkRef = checkRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val statement = new BoundStatement(preparedGetLastCheckStatus)
    statement.bind(_checkRef, _generation, _epoch)
    executeAsync(statement).map {
      case resultSet =>
        val row = resultSet.one()
        if (row == null) throw ApiException(ResourceNotFound) else {
          val timestamp = new DateTime(row.getDate(0), DateTimeZone.UTC)
          val lastUpdate = Option(row.getDate(1)).map(new DateTime(_, DateTimeZone.UTC))
          val lastChange = Option(row.getDate(2)).map(new DateTime(_, DateTimeZone.UTC))
          val condition = string2checkCondition(row.getString(3))
          val metrics = Option(row.getString(4))
            .map(string2checkMetrics)
            .map(_.metrics)
            .getOrElse(Map.empty[String,BigDecimal])
          CheckStatus(generation, timestamp, condition.lifecycle, condition.summary, condition.health, metrics,
            lastUpdate, lastChange, condition.correlation, condition.acknowledged, condition.squelched)
        }
    }
  }

  private val preparedDeleteCheckStatus = session.prepare(
    s"""
       |DELETE FROM $tableName
       |WHERE check_ref = ? AND generation = ? AND epoch = ?
     """.stripMargin)

  def deleteCheckStatus(checkRef: CheckRef, generation: Long, epoch: Long): Future[Unit] = {
    val _checkRef = checkRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val statement = new BoundStatement(preparedDeleteCheckStatus)
    statement.bind(_checkRef, _generation, _epoch)
    executeAsync(statement).map { _ => Unit }
  }

  def flushCheckStatus(): Future[Unit] = {
    executeAsync(s"TRUNCATE $tableName").map { _ => Unit }
  }

  def string2checkCondition(string: String): CheckCondition = string.parseJson.convertTo[CheckCondition]

  def checkCondition2string(condition: CheckCondition): String = condition.toJson.prettyPrint

  def string2checkNotifications(string: String): CheckNotifications = string.parseJson.convertTo[CheckNotifications]

  def checkNotifications2string(notifications: CheckNotifications): String = notifications.toJson.prettyPrint

  def string2checkMetrics(string: String): CheckMetrics = string.parseJson.convertTo[CheckMetrics]

  def checkMetrics2string(metrics: CheckMetrics): String = metrics.toJson.prettyPrint
}

case class CheckConditionHistory(conditions: Vector[CheckCondition])
case class CheckNotificationsHistory(notifications: Vector[CheckNotifications])
case class CheckMetricsHistory(metrics: Vector[CheckMetrics])