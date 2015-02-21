package io.mandelbrot.persistence.cassandra

import akka.actor.AddressFromURIString
import com.datastax.driver.core.{BoundStatement, Session}
import org.joda.time.DateTime
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConversions._

import io.mandelbrot.core.{ResourceNotFound, Conflict, ApiException}
import io.mandelbrot.core.entity._
import io.mandelbrot.persistence.cassandra.CassandraCoordinator.CassandraCoordinatorSettings

/**
 *
 */
class EntitiesDAL(settings: CassandraCoordinatorSettings, session: Session)(implicit ec: ExecutionContext) extends AbstractDriver(session, ec) {

  val tableName = "entities"

  // ensure that entities table exists
  session.execute(
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  shard_id int,
       |  entity_key text,
       |  created_at timestamp,
       |  PRIMARY KEY (shard_id, entity_key)
       |);
     """.stripMargin)

  private val preparedCreateEntity = session.prepare(
    s"""
       |INSERT INTO $tableName (shard_id, entity_key, created_at)
       |VALUES (?, ?, ?)
     """.stripMargin)

  def createEntity(op: CreateEntity, timestamp: DateTime): Future[CreateEntityResult] = {
    val shardId: java.lang.Integer = op.shardId
    val entityKey: String = op.entityKey
    session.executeAsync(new BoundStatement(preparedCreateEntity).bind(shardId, entityKey, timestamp.toDate)).map {
      case _ => CreateEntityResult(op)
    }
  }

  private val preparedGetEntity = session.prepare(
    s"""
       |SELECT shard_id, entity_key, created_at FROM $tableName
       |WHERE shard_id = ? AND entity_key = ?
     """.stripMargin)

  def getEntity(op: GetEntity): Future[GetEntityResult] = {
    val shardId: java.lang.Integer = op.shardId
    val entityKey: String = op.entityKey
    session.executeAsync(new BoundStatement(preparedGetEntity).bind(shardId, entityKey)).map { resultSet =>
      val row = resultSet.one()
      if (row != null) {
        val shardId = row.getInt(0)
        val entityKey = row.getString(1)
        GetEntityResult(op, shardId, entityKey)
      } else throw ApiException(ResourceNotFound)
    }
  }

  private val preparedDeleteEntity = session.prepare(
    s"""
       |DELETE FROM $tableName
       |WHERE shard_id = ? AND entity_key = ?
     """.stripMargin)

  def deleteEntity(op: DeleteEntity): Future[DeleteEntityResult] = {
    val shardId: java.lang.Integer = op.shardId
    val entityKey: String = op.entityKey
    session.executeAsync(new BoundStatement(preparedDeleteEntity).bind(shardId, entityKey)).map {
      case _ => DeleteEntityResult(op)
    }
  }

  private val preparedListEntities = session.prepare(
    s"""
       |SELECT shard_id, entity_key, created_at FROM $tableName
       |WHERE shard_id = ? AND entity_key > ?
       |ORDER BY entity_key ASC
       |LIMIT ?
     """.stripMargin)

  def listEntities(op: ListEntities): Future[ListEntitiesResult] = {
    val shardId: java.lang.Integer = op.shardId
    val entityKey: java.lang.String = op.token.map(_.entityKey).getOrElse("")
    val limit: java.lang.Integer = op.limit
    session.executeAsync(new BoundStatement(preparedListEntities).bind(shardId, entityKey, limit)).map { resultSet =>
      val entities = resultSet.all().map { row =>
        val shardId = row.getInt(0)
        val entityKey = row.getString(1)
        Entity(shardId, entityKey)
      }.toVector
      val token = if (entities.length < limit) None else entities.lastOption
      ListEntitiesResult(op, entities, token)
    }
  }

  def flushEntities(): Future[Unit] = {
    session.executeAsync(s"TRUNCATE $tableName").map { resultSet => }
  }
}