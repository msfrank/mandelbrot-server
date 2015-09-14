package io.mandelbrot.persistence.cassandra.dal

import com.datastax.driver.core.{BoundStatement, Session}
import com.datastax.driver.core.querybuilder.{Clause, QueryBuilder}
import spray.json._
import org.joda.time.{DateTime, DateTimeZone}
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConversions._
import java.util.Date

import io.mandelbrot.core.http.json.JsonProtocol._
import io.mandelbrot.core.model._
import io.mandelbrot.core.{ApiException, ResourceNotFound}
import io.mandelbrot.persistence.cassandra.{EpochUtils, CassandraStatePersisterSettings}

/**
 *
 */
class ProbeObservationDAL(settings: CassandraStatePersisterSettings,
                          val session: Session,
                          implicit val ec: ExecutionContext) extends AbstractDriver {


  val tableName: String = "probe_observation"

  session.execute(
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  probe_ref text,
       |  generation bigint,
       |  epoch bigint,
       |  timestamp timestamp,
       |  observation text,
       |  PRIMARY KEY ((probe_ref, generation, epoch), timestamp)
       |)
     """.stripMargin)

  private val preparedUpdateProbeObservation = session.prepare(
    s"""
       |INSERT INTO $tableName (probe_ref, generation, epoch, timestamp, observation)
       |VALUES (?, ?, ?, ?, ?)
     """.stripMargin)

  def updateProbeObservation(probeRef: ProbeRef, generation: Long, epoch: Long, observation: Observation): Future[Unit] = {
    val _probeRef = probeRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val timestamp = observation.timestamp.toDate
    val _observation = observation2string(observation)
    val statement = new BoundStatement(preparedUpdateProbeObservation)
    statement.bind(_probeRef, _generation, _epoch, timestamp, _observation)
    executeAsync(statement).map { _ => Unit }
  }

  private val preparedCheckIfEpochExhausted = session.prepare(
    s"""
       |SELECT timestamp
       |FROM $tableName
       |WHERE probe_ref = ? AND generation = ? AND epoch = ? AND timestamp > ?
       |LIMIT 1
     """.stripMargin)

  def checkIfEpochExhausted(probeRef: ProbeRef, generation: Long, epoch: Long, timestamp: DateTime): Future[Boolean] = {
    val _probeRef = probeRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val _timestamp = timestamp.toDate
    val statement = new BoundStatement(preparedCheckIfEpochExhausted)
    statement.bind(_probeRef, _generation, _epoch, _timestamp)
    executeAsync(statement).map {
      case resultSet => resultSet.isFullyFetched && resultSet.isExhausted
    }
  }

  private val preparedGetProbeObservation = session.prepare(
    s"""
       |SELECT observation
       |FROM $tableName
       |WHERE probe_ref = ? AND generation = ? AND epoch = ? AND timestamp = ?
     """.stripMargin)

  def getProbeObservation(probeRef: ProbeRef, generation: Long, epoch: Long, timestamp: DateTime): Future[ProbeObservation] = {
    val _probeRef = probeRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val _timestamp = timestamp.toDate
    val statement = new BoundStatement(preparedGetProbeObservation)
    statement.bind(_probeRef, _generation, _epoch, _timestamp)
    executeAsync(statement).map {
      case resultSet =>
        val row = resultSet.one()
        if (row == null) throw ApiException(ResourceNotFound) else {
          ProbeObservation(generation, string2observation(row.getString(0)))
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

  private val preparedGetProbeObservationHistory = session.prepare(
    s"""
       |SELECT observation
       |FROM $tableName
       |WHERE probe_ref = ? AND generation = ? AND epoch = ? AND timestamp >= ? AND timestamp < ?
       |LIMIT ?
     """.stripMargin)

  def getProbeObservationHistory(probeRef: ProbeRef,
                                 generation: Long,
                                 epoch: Long,
                                 from: Option[DateTime],
                                 to: Option[DateTime],
                                 limit: Int): Future[ProbeObservationHistory] = {
    val _probeRef = probeRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val start = from.map(_.toDate).getOrElse(EpochUtils.SMALLEST_DATE)
    val end = to.map(_.toDate).getOrElse(EpochUtils.LARGEST_DATE)
    val _limit: java.lang.Integer = limit
    val statement = new BoundStatement(preparedGetProbeObservationHistory)
    statement.bind(_probeRef, _generation, _epoch, start, end, _limit)
    statement.setFetchSize(limit)
    executeAsync(statement).map { resultSet =>
      val history = resultSet.all().map {
        row => ProbeObservation(generation, string2observation(row.getString(0)))
      }.toVector
      ProbeObservationHistory(history)
    }
  }

  /**
   *
   */
  def getProbeObservationHistory(probeRef: ProbeRef,
                                 generation: Long,
                                 epoch: Long,
                                 from: Option[DateTime],
                                 to: Option[DateTime],
                                 limit: Int,
                                 fromExclusive: Boolean,
                                 toInclusive: Boolean,
                                 descending: Boolean): Future[ProbeObservationHistory] = {
    val start = startClause(from, fromExclusive)
    val end = endClause(to, toInclusive)
    val ordering = if (descending) QueryBuilder.desc("timestamp") else QueryBuilder.asc("timestamp")
    val select = QueryBuilder.select("observation")
      .from(tableName)
      .where(QueryBuilder.eq("probe_ref", probeRef.toString))
      .and(QueryBuilder.eq("generation", generation: java.lang.Long))
      .and(QueryBuilder.eq("epoch", epoch))
      .and(start)
      .and(end)
      .orderBy(ordering)
      .limit(limit)
    select.setFetchSize(limit)
    executeAsync(select).map { resultSet =>
      val history = resultSet.all().map {
        row => ProbeObservation(generation, string2observation(row.getString(0)))
      }.toVector
      ProbeObservationHistory(history)
    }
  }

  private val preparedGetFirstProbeObservation = session.prepare(
    s"""
       |SELECT observation
       |FROM $tableName
       |WHERE probe_ref = ? AND generation = ? AND epoch = ?
       |ORDER BY timestamp ASC
       |LIMIT 1
     """.stripMargin)

  def getFirstProbeObservation(probeRef: ProbeRef, generation: Long, epoch: Long): Future[ProbeObservation] = {
    val _probeRef = probeRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val statement = new BoundStatement(preparedGetFirstProbeObservation)
    statement.bind(_probeRef, _generation, _epoch)
    executeAsync(statement).map {
      case resultSet =>
        val row = resultSet.one()
        if (row == null) throw ApiException(ResourceNotFound) else {
          ProbeObservation(generation, string2observation(row.getString(0)))
        }
    }
  }

  private val preparedGetLastProbeObservation = session.prepare(
    s"""
       |SELECT observation
       |FROM $tableName
       |WHERE probe_ref = ? AND generation = ? AND epoch = ?
       |ORDER BY timestamp DESC
       |LIMIT 1
     """.stripMargin)

  def getLastProbeObservation(probeRef: ProbeRef, generation: Long, epoch: Long): Future[ProbeObservation] = {
    val _probeRef = probeRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val statement = new BoundStatement(preparedGetLastProbeObservation)
    statement.bind(_probeRef, _generation, _epoch)
    executeAsync(statement).map {
      case resultSet =>
        val row = resultSet.one()
        if (row == null) throw ApiException(ResourceNotFound) else {
          ProbeObservation(generation, string2observation(row.getString(0)))
        }
    }
  }

  private val preparedDeleteProbeObservation = session.prepare(
    s"""
       |DELETE FROM $tableName
       |WHERE probe_ref = ? AND generation = ? AND epoch = ?
     """.stripMargin)

  def deleteProbeObservation(probeRef: ProbeRef, generation: Long, epoch: Long): Future[Unit] = {
    val _probeRef = probeRef.toString
    val _generation: java.lang.Long = generation
    val _epoch: java.lang.Long = epoch
    val statement = new BoundStatement(preparedDeleteProbeObservation)
    statement.bind(_probeRef, _generation, _epoch)
    executeAsync(statement).map { _ => Unit }
  }

  def flushProbeObservation(): Future[Unit] = {
    executeAsync(s"TRUNCATE $tableName").map { _ => Unit }
  }

  def string2observation(string: String): Observation = string.parseJson.convertTo[Observation]

  def observation2string(observation: Observation): String = observation.toJson.prettyPrint
}

case class ProbeObservationHistory(observations: Vector[ProbeObservation])
