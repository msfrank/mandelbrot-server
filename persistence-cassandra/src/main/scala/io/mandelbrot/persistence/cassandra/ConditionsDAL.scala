package io.mandelbrot.persistence.cassandra

import com.datastax.driver.core.{BoundStatement, Row, Session}
import io.mandelbrot.core.{ResourceNotFound, ApiException}
import org.joda.time.DateTime
import scala.concurrent.{Future, ExecutionContext}
import scala.collection.JavaConversions._
import java.util.Date

import io.mandelbrot.core.state.{UpdateProbeStatus, ProbeCondition}
import io.mandelbrot.core.system._
import io.mandelbrot.persistence.cassandra.CassandraPersister.CassandraPersisterSettings

/**
 *
 */
class ConditionsDAL(settings: CassandraPersisterSettings, session: Session)(implicit ec: ExecutionContext) extends AbstractDriver(session, ec) {

  val tableName = "conditions"

  val LARGEST_DATE = new Date(java.lang.Long.MAX_VALUE)
  val SMALLEST_DATE = new Date(0)

  session.execute(
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  probe_ref text,
       |  epoch bigint,
       |  seq_num bigint,
       |  timestamp timestamp,
       |  lifecycle text,
       |  health text,
       |  summary text,
       |  correlation uuid,
       |  acknowledged uuid,
       |  squelched boolean,
       |  PRIMARY KEY (probe_ref, epoch, seq_num)
       |)
     """.stripMargin)

  private val preparedInsertCondition = session.prepare(
    s"""
       |INSERT INTO $tableName (probe_ref, epoch, seq_num, timestamp, lifecycle, health, summary, correlation, acknowledged, squelched)
       |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     """.stripMargin)

  def insertCondition(op: UpdateProbeStatus, epoch: Long): Future[Unit] = {
    val probeRef = op.probeRef.toString
    val _epoch: java.lang.Long = epoch
    val seqNum: java.lang.Long = op.seqNum
    val timestamp = op.status.timestamp.toDate
    val lifecycle = op.status.lifecycle.toString
    val health = op.status.health.toString
    val summary = op.status.summary.orNull
    val correlation = op.status.correlation.orNull
    val acknowledged = op.status.acknowledged.orNull
    val squelched: java.lang.Boolean = op.status.squelched
    session.executeAsync(new BoundStatement(preparedInsertCondition).bind(probeRef,
      _epoch, seqNum, timestamp, lifecycle, health, summary, correlation, acknowledged,
      squelched)).map { _ => Unit }
  }

  private val preparedGetCondition = session.prepare(
    s"""
       |SELECT timestamp, lifecycle, health, summary, correlation, acknowledged, squelched
       |FROM $tableName
       |WHERE probe_ref = ? AND epoch = ? AND timestamp >= ? AND timestamp < ?
       |LIMIT ?
     """.stripMargin)

  def getCondition(probeRef: ProbeRef, epoch: Long, seqNum: Long): Future[ProbeCondition] = {
    val _probeRef = probeRef.toString
    val _epoch: java.lang.Long = epoch
    val _seqNum: java.lang.Long = seqNum
    session.executeAsync(new BoundStatement(preparedGetCondition).bind(_probeRef, _epoch, _seqNum)).map {
      case resultSet =>
        val row = resultSet.one()
        if (row != null) row2ProbeCondition(resultSet.one()) else throw ApiException(ResourceNotFound)
    }
  }
  
  private val preparedDeleteConditionEpoch = session.prepare(
    s"""
       |DELETE FROM $tableName
       |WHERE probe_ref = ? AND epoch = ?
     """.stripMargin)

  def deleteConditionEpoch(probeRef: ProbeRef, epoch: Long): Future[Unit] = {
    val _probeRef = probeRef.toString
    val _epoch: java.lang.Long = epoch
    session.executeAsync(new BoundStatement(preparedDeleteConditionEpoch).bind(_probeRef, _epoch)).map { _ => Unit }
  }

  private val preparedGetFirstConditionEpoch = session.prepare(
    s"""
       |SELECT epoch from $tableName
       |WHERE probe_ref = ?
       |ORDER BY epoch ASC
       |LIMIT 1
     """.stripMargin)

  def getFirstConditionEpoch(probeRef: ProbeRef): Future[Option[Long]] = {
    session.executeAsync(new BoundStatement(preparedGetFirstConditionEpoch).bind(probeRef.toString)).map { resultSet =>
      val row = resultSet.one()
      if (row != null) Some(row.getLong(0)) else None
    }
  }

  private val preparedGetLastConditionEpoch = session.prepare(
    s"""
       |SELECT epoch from $tableName
       |WHERE probe_ref = ?
       |ORDER BY epoch DESC
       |LIMIT 1
     """.stripMargin)

  def getLastConditionEpoch(probeRef: ProbeRef): Future[Option[Long]] = {
    session.executeAsync(new BoundStatement(preparedGetLastConditionEpoch).bind(probeRef.toString)).map { resultSet =>
      val row = resultSet.one()
      if (row != null) Some(row.getLong(0)) else None
    }
  }

  private val preparedGetConditionHistory = session.prepare(
    s"""
       |SELECT probe_ref, timestamp, lifecycle, health, summary, correlation, acknowledged, squelched
       |FROM $tableName
       |WHERE probe_ref = ? AND epoch = ? AND timestamp >= ? AND timestamp < ?
       |LIMIT ?
     """.stripMargin)

  def getConditionHistory(probeRef: ProbeRef, epoch: Long, from: Option[DateTime], to: Option[DateTime], limit: Int): Future[Vector[ProbeCondition]] = {
    val _probeRef = probeRef.toString
    val _epoch: java.lang.Long = epoch
    val start = from.map(_.toDate).getOrElse(SMALLEST_DATE)
    val end = to.map(_.toDate).getOrElse(LARGEST_DATE)
    val _limit: java.lang.Integer = limit
    session.executeAsync(new BoundStatement(preparedGetConditionHistory).bind(_probeRef, _epoch, start, end, _limit)).map { resultSet =>
      resultSet.all().map(row2ProbeCondition).toVector
    }
  }

  def flushConditions(): Future[Unit] = {
    session.executeAsync(s"TRUNCATE $tableName").map { _ => Unit }
  }

  def row2ProbeCondition(row: Row): ProbeCondition = {
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
    val correlation = Option(row.getUUID(7))
    val acknowledged = Option(row.getUUID(8))
    val squelched = row.getBool(9)
    ProbeCondition(timestamp, lifecycle, summary, health, correlation, acknowledged, squelched)
  }
}
