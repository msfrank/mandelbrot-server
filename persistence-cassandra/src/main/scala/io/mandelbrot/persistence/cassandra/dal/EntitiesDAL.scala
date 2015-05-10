package io.mandelbrot.persistence.cassandra.dal

import com.datastax.driver.core.{BoundStatement, Session}
import io.mandelbrot.core.entity._
import io.mandelbrot.core.{ApiException, ResourceNotFound}
import io.mandelbrot.persistence.cassandra.CassandraEntityCoordinatorSettings
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}

/**
 *
 */
class EntitiesDAL(settings: CassandraEntityCoordinatorSettings,
                  val session: Session,
                  implicit val ec: ExecutionContext) extends AbstractDriver {

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
    executeAsync(new BoundStatement(preparedCreateEntity).bind(shardId, entityKey, timestamp.toDate)).map {
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
    executeAsync(new BoundStatement(preparedGetEntity).bind(shardId, entityKey)).map { resultSet =>
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
    executeAsync(new BoundStatement(preparedDeleteEntity).bind(shardId, entityKey)).map {
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
    executeAsync(new BoundStatement(preparedListEntities).bind(shardId, entityKey, limit)).map { resultSet =>
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
    executeAsync(s"TRUNCATE $tableName").map { resultSet => }
  }
}