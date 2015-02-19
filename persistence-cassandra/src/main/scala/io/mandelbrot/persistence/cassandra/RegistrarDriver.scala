package io.mandelbrot.persistence.cassandra

import java.net.URI

import com.datastax.driver.core.{BoundStatement, Session}
import org.joda.time.DateTime
import scala.concurrent.{Future, ExecutionContext}
import scala.collection.JavaConversions._

import io.mandelbrot.core.registry._
import io.mandelbrot.core.http.JsonProtocol
import io.mandelbrot.core.{ApiException, ResourceNotFound}

import io.mandelbrot.persistence.cassandra.CassandraRegistrar.CassandraRegistrarSettings

/**
 *
 */
class RegistrarDriver(settings: CassandraRegistrarSettings, session: Session)(implicit ec: ExecutionContext) extends AbstractDriver(session, ec) {
  import spray.json._
  import JsonProtocol._

  val tableName: String = "registry"

  session.execute(
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  p int,
       |  uri text,
       |  registration text,
       |  lsn bigint,
       |  last_update timestamp,
       |  joined_on timestamp,
       |  PRIMARY KEY(p, uri)
       |);
     """.stripMargin)

  val preparedCreateProbeSystem = session.prepare(
    s"""
       |INSERT INTO $tableName (p, uri, registration, lsn, last_update, joined_on)
       |VALUES (0, ?, ?, 1, ?, ?)
     """.stripMargin)

  def createProbeSystem(op: CreateProbeSystemEntry, timestamp: DateTime): Future[CreateProbeSystemEntryResult] = {
    val uri = op.uri.toString
    val registration = op.registration.toJson.toString()
    val _timestamp = timestamp.toDate
    session.executeAsync(new BoundStatement(preparedCreateProbeSystem).bind(uri, registration, _timestamp, _timestamp)).map {
      resultSet => CreateProbeSystemEntryResult(op, 1)
    }
  }

  val preparedUpdateProbeSystem = session.prepare(
    s"""
       |UPDATE $tableName
       |SET registration = ?, lsn = ?, last_update = ?
       |WHERE p = 0 AND uri = ?
     """.stripMargin)

  def updateProbeSystem(op: UpdateProbeSystemEntry, timestamp: DateTime): Future[UpdateProbeSystemEntryResult] = {
    val uri = op.uri.toString
    val registration = op.registration.toJson.toString()
    val _timestamp = timestamp.toDate
    val lsn: java.lang.Long = op.lsn + 1
    session.executeAsync(new BoundStatement(preparedUpdateProbeSystem).bind(registration, lsn, _timestamp, uri)).map {
      resultSet => UpdateProbeSystemEntryResult(op, lsn)
    }
  }

  val preparedDeleteProbeSystem = session.prepare(
    s"""
       |DELETE FROM $tableName
       |WHERE p = 0 AND uri = ?
     """.stripMargin)

  def deleteProbeSystem(op: DeleteProbeSystemEntry): Future[DeleteProbeSystemEntryResult] = {
    val uri = op.uri.toString
    session.executeAsync(new BoundStatement(preparedDeleteProbeSystem).bind(uri)).map {
      resultSet => DeleteProbeSystemEntryResult(op, op.lsn)
    }
  }

  val preparedGetProbeSystem = session.prepare(
    s"""
       |SELECT registration, lsn FROM $tableName
       |WHERE p = 0 AND uri = ?
     """.stripMargin)

  def getProbeSystem(op: GetProbeSystemEntry): Future[GetProbeSystemEntryResult] = {
    val uri = op.uri.toString
    session.executeAsync(new BoundStatement(preparedGetProbeSystem).bind(uri)).map { resultSet =>
      val row = resultSet.one()
      if (row != null) {
        val registration = JsonParser(row.getString(0)).convertTo[ProbeRegistration]
        val lsn = row.getLong(1)
        GetProbeSystemEntryResult(op, registration, lsn)
      } else throw new ApiException(ResourceNotFound)
    }
  }

  val preparedListProbeSystems = session.prepare(
    s"""
       |SELECT uri, lsn, last_update, joined_on FROM $tableName
       |WHERE p = 0 AND uri > ?
       |ORDER BY uri ASC
       |LIMIT ?
     """.stripMargin)

  def listProbeSystems(op: ListProbeSystems): Future[ListProbeSystemsResult] = {
    val uri = op.token.map(_.toString).getOrElse("")
    val limit: java.lang.Integer = op.limit
    session.executeAsync(new BoundStatement(preparedListProbeSystems).bind(uri, limit)).map { resultSet =>
      val systems = resultSet.all().map { row =>
        val uri = new URI(row.getString(0))
        val lsn = row.getLong(1)
        val lastUpdate = new DateTime(row.getDate(2))
        val joinedOn = new DateTime(row.getDate(3))
        uri -> ProbeSystemMetadata(lastUpdate, joinedOn)
      }.toVector
      val token = if (systems.length < limit) None else systems.lastOption.map(_._1)
      ListProbeSystemsResult(op, systems.toMap, token)
    }
  }

  def flushEntities(): Future[Unit] = {
    session.executeAsync(s"TRUNCATE $tableName").map { resultSet => }
  }
}
