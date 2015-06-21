package io.mandelbrot.persistence.cassandra.dal

import com.datastax.driver.core.{BoundStatement, Session}
import spray.json._
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConversions._

import io.mandelbrot.core.http.json.JsonProtocol._
import io.mandelbrot.core.model._
import io.mandelbrot.persistence.cassandra.CassandraRegistryPersisterSettings

/**
 *
 */
class AgentGroupDAL(settings: CassandraRegistryPersisterSettings,
                    val session: Session,
                    implicit val ec: ExecutionContext) extends AbstractDriver {

  val tableName: String = "agent_group"

  session.execute(
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  group_name text,
       |  agent_id text,
       |  metadata text,
       |  PRIMARY KEY(group_name, agent_id)
       |);
     """.stripMargin)

  private val preparedAddToGroup = session.prepare(
    s"""
       |INSERT INTO $tableName (group_name, agent_id, metadata)
       |VALUES (?, ?, ?)
     """.stripMargin)

  def addToGroup(groupName: String, metadata: AgentMetadata): Future[Unit] = {
    val _groupName: java.lang.String = groupName
    val _agentId = metadata.agentId.toString
    val _metadata = agentMetadata2string(metadata)
    executeAsync(new BoundStatement(preparedAddToGroup)
      .bind(_groupName, _agentId, _metadata))
      .map { _ => }
  }

  private val preparedRemoveFromGroup = session.prepare(
    s"""
       |DELETE FROM $tableName
       |WHERE group_name = ? AND agent_id = ?
     """.stripMargin)

  def removeFromGroup(groupName: String, agentId: AgentId): Future[Unit] = {
    val _groupName: java.lang.String = groupName
    val _agentId = agentId.toString
    executeAsync(new BoundStatement(preparedRemoveFromGroup)
      .bind(_groupName, _agentId))
      .map { _ => }
  }

  def flushGroups(): Future[Unit] = {
    executeAsync(s"TRUNCATE $tableName").map { _ => }
  }

  def string2agentMetadata(string: String): AgentMetadata = string.parseJson.convertTo[AgentMetadata]

  def agentMetadata2string(metadata: AgentMetadata): String = metadata.toJson.prettyPrint
}
