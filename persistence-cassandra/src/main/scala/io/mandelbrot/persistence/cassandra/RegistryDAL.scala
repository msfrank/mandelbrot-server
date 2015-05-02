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

/**
 *
 */
class RegistryDAL(settings: CassandraRegistryPersisterSettings,
                  val session: Session,
                  implicit val ec: ExecutionContext) extends AbstractDriver {

  val tableName: String = "registry"

  session.execute(
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  p int,
       |  agent_id text,
       |  registration text,
       |  lsn bigint,
       |  last_update timestamp,
       |  joined_on timestamp,
       |  PRIMARY KEY(p, agent_id)
       |);
     """.stripMargin)

  private val preparedCreateAgent = session.prepare(
    s"""
       |INSERT INTO $tableName (p, agent_id, registration, lsn, last_update, joined_on)
       |VALUES (0, ?, ?, 1, ?, ?)
     """.stripMargin)

  def createAgent(op: CreateRegistration, timestamp: DateTime): Future[CreateRegistrationResult] = {
    val agentId = op.agentId.toString
    val registration = op.registration.toJson.toString()
    val _timestamp = timestamp.toDate
    executeAsync(new BoundStatement(preparedCreateAgent).bind(agentId, registration, _timestamp, _timestamp)).map {
      resultSet => CreateRegistrationResult(op, AgentMetadata(op.agentId, timestamp, timestamp, 1))
    }
  }

  private val preparedUpdateAgent = session.prepare(
    s"""
       |UPDATE $tableName
       |SET registration = ?, lsn = ?, last_update = ?
       |WHERE p = 0 AND agent_id = ?
     """.stripMargin)

  def updateAgent(op: UpdateRegistration, timestamp: DateTime): Future[UpdateRegistrationResult] = {
    val agentId = op.agentId.toString
    val registration = op.registration.toJson.toString()
    val _timestamp = timestamp.toDate
    val lsn: java.lang.Long = op.lsn + 1
    executeAsync(new BoundStatement(preparedUpdateAgent).bind(registration, lsn, _timestamp, agentId)).map {
      resultSet => UpdateRegistrationResult(op, AgentMetadata(op.agentId, timestamp, timestamp, lsn))
    }
  }

  private val preparedDeleteAgent = session.prepare(
    s"""
       |DELETE FROM $tableName
       |WHERE p = 0 AND agent_id = ?
     """.stripMargin)

  def deleteAgent(op: DeleteRegistration): Future[DeleteRegistrationResult] = {
    val agentId = op.agentId.toString
    executeAsync(new BoundStatement(preparedDeleteAgent).bind(agentId)).map {
      resultSet => DeleteRegistrationResult(op, op.lsn)
    }
  }

  private val preparedGetAgent = session.prepare(
    s"""
       |SELECT registration, lsn FROM $tableName
       |WHERE p = 0 AND uri = ?
     """.stripMargin)

  def getAgent(op: GetRegistration): Future[GetRegistrationResult] = {
    val agentId = op.agentId.toString
    executeAsync(new BoundStatement(preparedGetAgent).bind(agentId)).map { resultSet =>
      val row = resultSet.one()
      if (row != null) {
        val registration = JsonParser(row.getString(0)).convertTo[AgentRegistration]
        val lsn = row.getLong(1)
        GetRegistrationResult(op, registration, lsn)
      } else throw ApiException(ResourceNotFound)
    }
  }

  private val preparedListAgents = session.prepare(
    s"""
       |SELECT agent_id, lsn, last_update, joined_on FROM $tableName
       |WHERE p = 0 AND agent_id > ?
       |ORDER BY agent_id ASC
       |LIMIT ?
     """.stripMargin)

  def listAgents(op: ListRegistrations): Future[ListRegistrationsResult] = {
    val last = op.last.map(_.toString).getOrElse("")
    val limit: java.lang.Integer = op.limit
    executeAsync(new BoundStatement(preparedListAgents).bind(last, limit)).map { resultSet =>
      val agents = resultSet.all().map { row =>
        val agentId = AgentId(row.getString(0))
        val lsn = row.getLong(1)
        val lastUpdate = new DateTime(row.getDate(2))
        val joinedOn = new DateTime(row.getDate(3))
        AgentMetadata(agentId, joinedOn, lastUpdate, lsn)
      }.toVector
      val token = if (agents.length < limit) None else agents.lastOption.map(_.agentId.toString)
      ListRegistrationsResult(op, AgentsPage(agents, token))
    }
  }

  def flushEntities(): Future[Unit] = {
    executeAsync(s"TRUNCATE $tableName").map { resultSet => }
  }
}
