package io.mandelbrot.persistence.cassandra

import akka.util.ByteString
import com.datastax.driver.core.{BoundStatement, Row, Session}
import scala.concurrent.{ExecutionContext, Future}
import org.joda.time.{DateTimeZone, DateTime}

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
       |  seq_num bigint,
       |  timestamp timestamp,
       |  last_update timestamp,
       |  last_change timestamp
       |)
     """.stripMargin)

  private val preparedGetProbeState = session.prepare(
    s"""
       |SELECT probe_ref, seq_num, timestamp, last_update, last_change
       |FROM $tableName
       |WHERE probe_ref = ?
     """.stripMargin)

  def initializeProbeState(probeRef: ProbeRef): Future[ProbeState] = {
    session.executeAsync(new BoundStatement(preparedGetProbeState).bind(probeRef.toString)).map { resultSet =>
      val row = resultSet.one()
      if (row != null) row2ProbeState(row) else ProbeState(probeRef, 0, DateTime.now(DateTimeZone.UTC), None, None)
    }
  }

  def getProbeState(probeRef: ProbeRef): Future[ProbeState] = {
    session.executeAsync(new BoundStatement(preparedGetProbeState).bind(probeRef.toString)).map { resultSet =>
      val row = resultSet.one()
      if (row != null) row2ProbeState(row) else throw ApiException(ResourceNotFound)
    }
  }

  private val preparedUpdateProbeState = session.prepare(
    s"""
       |INSERT INTO $tableName (probe_ref, seq_num, timestamp, last_update, last_change)
       |VALUES (?, ?, ?, ?, ?)
     """.stripMargin)

  def updateProbeState(probeState: ProbeState): Future[Unit] = {
    val probeRef = probeState.probeRef.toString
    val seqNum: java.lang.Long = probeState.seqNum
    val timestamp = probeState.timestamp.toDate
    val lastUpdate = probeState.lastUpdate.map(_.toDate).orNull
    val lastChange = probeState.lastChange.map(_.toDate).orNull
    session.executeAsync(new BoundStatement(preparedUpdateProbeState).bind(probeRef,
      seqNum, timestamp, lastUpdate, lastChange)).map { _ => Unit }
  }

  private val preparedDeleteProbeState = session.prepare(
    s"""
       |DELETE FROM $tableName WHERE probe_ref = ?
     """.stripMargin)

  def deleteProbeState(probeRef: ProbeRef): Future[Unit] = {
    session.executeAsync(new BoundStatement(preparedDeleteProbeState).bind(probeRef.toString)).map { _ => Unit }
  }

  def flushProbeState(): Future[Unit] = {
    session.executeAsync(s"TRUNCATE $tableName").map { _ => Unit }
  }

  implicit def row2ProbeState(row: Row): ProbeState = {
    val probeRef = ProbeRef(row.getString(0))
    val seqNum = row.getLong(1)
    val timestamp = new DateTime(row.getDate(2))
    val lastUpdate = Option(row.getDate(3)).map(new DateTime(_))
    val lastChange = Option(row.getDate(4)).map(new DateTime(_))
    ProbeState(probeRef, seqNum, timestamp, lastUpdate, lastChange)
  }
}

case class ProbeState(probeRef: ProbeRef, seqNum: Long, timestamp: DateTime, lastUpdate: Option[DateTime], lastChange: Option[DateTime])

