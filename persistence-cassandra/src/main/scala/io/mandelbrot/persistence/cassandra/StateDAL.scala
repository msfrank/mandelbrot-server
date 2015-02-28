package io.mandelbrot.persistence.cassandra

import akka.util.ByteString
import com.datastax.driver.core.{BoundStatement, Row, Session}
import scala.concurrent.{ExecutionContext, Future}
import org.joda.time.DateTime

import io.mandelbrot.core.{ApiException, ResourceNotFound}
import io.mandelbrot.core.state._
import io.mandelbrot.core.system._
import io.mandelbrot.persistence.cassandra.CassandraPersister.CassandraPersisterSettings

/**
 *
 */
class StateDAL(settings: CassandraPersisterSettings, session: Session)(implicit ec: ExecutionContext) extends AbstractDriver(session, ec) {
  import scala.language.implicitConversions

  val tableName: String = "state"

  session.execute(
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
     """.stripMargin)

  private val preparedGetProbeState = session.prepare(
    s"""
       |SELECT probe_ref, generation, timestamp, lifecycle, health, summary, last_update, last_change, correlation, acknowledged, squelched, context
       |FROM $tableName
       |WHERE probe_ref = ?
     """.stripMargin)

  def initializeProbeState(op: InitializeProbeStatus): Future[InitializeProbeStatusResult] = {
    val probeRef = op.ref.toString
    session.executeAsync(new BoundStatement(preparedGetProbeState).bind(probeRef)).map { resultSet =>
      val row = resultSet.one()
      if (row != null) {
        val state = row2ProbeState(row)
        InitializeProbeStatusResult(op, state.status, state.lsn)
      } else {
        val status = ProbeStatus(op.timestamp, ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
        InitializeProbeStatusResult(op, status, 0)
      }
    }
  }

  def getProbeState(op: GetProbeState): Future[GetProbeStateResult] = {
    val probeRef = op.probeRef.toString
    session.executeAsync(new BoundStatement(preparedGetProbeState).bind(probeRef)).map { resultSet =>
      val row = resultSet.one()
      if (row != null) {
        val state = row2ProbeState(row)
        GetProbeStateResult(op, state.status, state.lsn)
      } else throw ApiException(ResourceNotFound)
    }
  }

  private val preparedUpdateProbeState = session.prepare(
    s"""
       |INSERT INTO $tableName (probe_ref, generation, timestamp, lifecycle, health, summary, last_update, last_change, correlation, acknowledged, squelched, context)
       |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     """.stripMargin)

  def updateProbeState(op: UpdateProbeStatus): Future[UpdateProbeStatusResult] = {
    val probeRef = op.ref.toString
    val lsn: java.lang.Long = op.lsn
    val timestamp = op.status.timestamp.toDate
    val lifecycle = op.status.lifecycle.toString
    val health = op.status.health.toString
    val summary = op.status.summary.orNull
    val lastUpdate = op.status.lastUpdate.map(_.toDate).orNull
    val lastChange = op.status.lastChange.map(_.toDate).orNull
    val correlation = op.status.correlation.orNull
    val acknowledged = op.status.acknowledged.orNull
    val squelched: java.lang.Boolean = op.status.squelched
    val context = null
    session.executeAsync(new BoundStatement(preparedUpdateProbeState).bind(probeRef, lsn,
      timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlation, acknowledged, squelched, context
    )).map {
      resultSet => UpdateProbeStatusResult(op)
    }
  }

  private val preparedDeleteProbeState = session.prepare(
    s"""
       |DELETE FROM $tableName WHERE probe_ref = ?
     """.stripMargin)

  def deleteProbeState(op: DeleteProbeStatus): Future[DeleteProbeStatusResult] = {
    val probeRef = op.ref.toString
    session.executeAsync(new BoundStatement(preparedDeleteProbeState).bind(probeRef)).map {
      resultSet => DeleteProbeStatusResult(op)
    }
  }

  def flushProbeState(): Future[Unit] = {
    session.executeAsync(s"TRUNCATE $tableName").map { resultSet => }
  }

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
    val status = ProbeStatus(timestamp, lifecycle, summary, health, Map.empty, lastUpdate, lastChange, correlation, acknowledged, squelched)
    ProbeState(status, generation, None)
  }
}
