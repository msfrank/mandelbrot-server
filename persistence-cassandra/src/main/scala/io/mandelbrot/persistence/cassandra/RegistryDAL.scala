package io.mandelbrot.persistence.cassandra

import com.datastax.driver.core.{BoundStatement, Session}
import org.joda.time.DateTime
import spray.json._
import scala.concurrent.{Future, ExecutionContext}
import scala.collection.JavaConversions._
import java.net.URI

import io.mandelbrot.core.registry._
import io.mandelbrot.core.model._
import io.mandelbrot.core.http.json.JsonProtocol._
import io.mandelbrot.core.{ApiException, ResourceNotFound}

import io.mandelbrot.persistence.cassandra.CassandraRegistrar.CassandraRegistrarSettings

/**
 *
 */
class RegistryDAL(settings: CassandraRegistrarSettings,
                  val session: Session,
                  implicit val ec: ExecutionContext) extends AbstractDriver {

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

  private val preparedCreateProbeSystem = session.prepare(
    s"""
       |INSERT INTO $tableName (p, uri, registration, lsn, last_update, joined_on)
       |VALUES (0, ?, ?, 1, ?, ?)
     """.stripMargin)

  def createProbeSystem(op: CreateProbeSystemEntry, timestamp: DateTime): Future[CreateProbeSystemEntryResult] = {
    val uri = op.uri.toString
    val registration = op.registration.toJson.toString()
    val _timestamp = timestamp.toDate
    executeAsync(new BoundStatement(preparedCreateProbeSystem).bind(uri, registration, _timestamp, _timestamp)).map {
      resultSet => CreateProbeSystemEntryResult(op, 1)
    }
  }

  private val preparedUpdateProbeSystem = session.prepare(
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
    executeAsync(new BoundStatement(preparedUpdateProbeSystem).bind(registration, lsn, _timestamp, uri)).map {
      resultSet => UpdateProbeSystemEntryResult(op, lsn)
    }
  }

  private val preparedDeleteProbeSystem = session.prepare(
    s"""
       |DELETE FROM $tableName
       |WHERE p = 0 AND uri = ?
     """.stripMargin)

  def deleteProbeSystem(op: DeleteProbeSystemEntry): Future[DeleteProbeSystemEntryResult] = {
    val uri = op.uri.toString
    executeAsync(new BoundStatement(preparedDeleteProbeSystem).bind(uri)).map {
      resultSet => DeleteProbeSystemEntryResult(op, op.lsn)
    }
  }

  private val preparedGetProbeSystem = session.prepare(
    s"""
       |SELECT registration, lsn FROM $tableName
       |WHERE p = 0 AND uri = ?
     """.stripMargin)

  def getProbeSystem(op: GetProbeSystemEntry): Future[GetProbeSystemEntryResult] = {
    val uri = op.uri.toString
    executeAsync(new BoundStatement(preparedGetProbeSystem).bind(uri)).map { resultSet =>
      val row = resultSet.one()
      if (row != null) {
        val registration = JsonParser(row.getString(0)).convertTo[ProbeRegistration]
        val lsn = row.getLong(1)
        GetProbeSystemEntryResult(op, registration, lsn)
      } else throw ApiException(ResourceNotFound)
    }
  }

  private val preparedListProbeSystems = session.prepare(
    s"""
       |SELECT uri, lsn, last_update, joined_on FROM $tableName
       |WHERE p = 0 AND uri > ?
       |ORDER BY uri ASC
       |LIMIT ?
     """.stripMargin)

  def listProbeSystems(op: ListProbeSystems): Future[ListProbeSystemsResult] = {
    val last = op.last.map(_.toString).getOrElse("")
    val limit: java.lang.Integer = op.limit
    executeAsync(new BoundStatement(preparedListProbeSystems).bind(last, limit)).map { resultSet =>
      val systems = resultSet.all().map { row =>
        val uri = new URI(row.getString(0))
        val lsn = row.getLong(1)
        val lastUpdate = new DateTime(row.getDate(2))
        val joinedOn = new DateTime(row.getDate(3))
        ProbeSystemMetadata(uri, lastUpdate, joinedOn)
      }.toVector
      val token = if (systems.length < limit) None else systems.lastOption.map(_.uri.toString)
      ListProbeSystemsResult(op, ProbeSystemsPage(systems, token))
    }
  }

  def flushEntities(): Future[Unit] = {
    executeAsync(s"TRUNCATE $tableName").map { resultSet => }
  }
}
