package io.mandelbrot.persistence.cassandra

import akka.actor.AddressFromURIString
import com.datastax.driver.core.{BatchStatement, BoundStatement, Session}
import org.joda.time.DateTime
import com.google.common.util.concurrent.{Futures, FutureCallback, ListenableFuture}
import scala.concurrent.{ExecutionContext, Promise, Future}
import scala.collection.JavaConversions._

import io.mandelbrot.core.{ResourceNotFound, Conflict, ApiException}
import io.mandelbrot.core.cluster._
import io.mandelbrot.persistence.cassandra.CassandraCoordinator.CassandraCoordinatorSettings

/**
 *
 */
class CoordinatorDriver(settings: CassandraCoordinatorSettings, session: Session)(implicit ec: ExecutionContext) {

  val shardsTableName = "shards"
  val entitiesTableName = "entities"

  // ensure that shards and entities tables exist
  session.execute(
    s"""
       |CREATE TABLE IF NOT EXISTS $shardsTableName (
       |  p int,
       |  shard_id int,
       |  address text,
       |  last_update timestamp,
       |  PRIMARY KEY (p, shard_id)
       |);
     """.stripMargin)
  session.execute(
    s"""
       |CREATE TABLE IF NOT EXISTS $entitiesTableName (
       |  shard_id int,
       |  entity_key text,
       |  created_at timestamp,
       |  PRIMARY KEY (shard_id, entity_key)
       |);
     """.stripMargin)

  def guavaFutureToAkka[T](f: ListenableFuture[T]): Future[T] = {
    val p = Promise[T]()
    Futures.addCallback(f,
      new FutureCallback[T] {
        def onSuccess(r: T) = p success r
        def onFailure(t: Throwable) = p failure t
      })
    p.future
  }

  val preparedCreateShard = session.prepare(
    s"""
       |INSERT INTO $shardsTableName (p, shard_id, address, last_update)
       |VALUES (0, ?, ?, ?)
       |IF NOT EXISTS
     """.stripMargin)

  def createShard(op: CreateShard, timestamp: DateTime): Future[CreateShardResult] = {
    val shardId: java.lang.Integer = op.shardId
    val address: String = op.address.toString
    val f = session.executeAsync(new BoundStatement(preparedCreateShard).bind(shardId, address, timestamp.toDate))
    guavaFutureToAkka(f).map {
      case resultSet if !resultSet.wasApplied() => throw ApiException(Conflict)
      case _ => CreateShardResult(op)
    }
  }

  val preparedUpdateShard = session.prepare(
    s"""
       |UPDATE $shardsTableName
       |SET address = ?, last_update = ?
       |WHERE p = 0 AND shard_id = ?
       |IF address = ?
     """.stripMargin)

  def updateShard(op: UpdateShard, timestamp: DateTime): Future[UpdateShardResult] = {
    val shardId: java.lang.Integer = op.shardId
    val address: String = op.address.toString
    val prev: String = op.prev.toString
    val f = session.executeAsync(new BoundStatement(preparedUpdateShard).bind(address, timestamp.toDate, shardId, prev))
    guavaFutureToAkka(f).map {
      case resultSet if !resultSet.wasApplied() => throw ApiException(Conflict)
      case _ => UpdateShardResult(op)
    }
  }

  val preparedGetShard = session.prepare(
    s"""
       |SELECT shard_id, address, last_update FROM $shardsTableName
       |WHERE p = 0 AND shard_id = ?
     """.stripMargin)

  def getShard(op: GetShard): Future[GetShardResult] = {
    val shardId: java.lang.Integer = op.shardId
    val f = session.executeAsync(new BoundStatement(preparedGetShard).bind(shardId))
    guavaFutureToAkka(f).map { resultSet =>
      val row = resultSet.one()
      if (row != null) {
        val shardId = row.getInt(0)
        val address = AddressFromURIString(row.getString(1))
        val lastUpdate = new DateTime(row.getDate(2))
        GetShardResult(op, shardId, address)
      } else throw ApiException(ResourceNotFound)
    }
  }

  val preparedListShards = session.prepare(
    s"""
       |SELECT shard_id, address, last_update FROM $shardsTableName
       |WHERE p = 0 AND shard_id >= ?
       |ORDER BY shard_id ASC
       |LIMIT ?
     """.stripMargin)

  def listShards(op: ListShards): Future[ListShardsResult] = {
    val shardId: java.lang.Integer = op.token.map(1 + _.shardId: java.lang.Integer).getOrElse(0)
    val limit: java.lang.Integer = op.limit
    val f = session.executeAsync(new BoundStatement(preparedListShards).bind(shardId, limit))
    guavaFutureToAkka(f).map { resultSet =>
      val shards = resultSet.all().map { row =>
        val shardId = row.getInt(0)
        val address = AddressFromURIString(row.getString(1))
        Shard(shardId, address)
      }.toVector
      val token = if (shards.length < limit) None else shards.lastOption
      ListShardsResult(op, shards, token)
    }
  }

  def flushShards(): Future[Unit] = {
    val f = session.executeAsync(s"TRUNCATE $shardsTableName")
    guavaFutureToAkka(f).map { resultSet => }
  }

  val preparedCreateEntity = session.prepare(
    s"""
       |INSERT INTO $entitiesTableName (shard_id, entity_key, created_at)
       |VALUES (?, ?, ?)
     """.stripMargin)

  def createEntity(op: CreateEntity, timestamp: DateTime): Future[CreateEntityResult] = {
    val shardId: java.lang.Integer = op.shardId
    val entityKey: String = op.entityKey
    val f = session.executeAsync(new BoundStatement(preparedCreateEntity).bind(shardId, entityKey, timestamp.toDate))
    guavaFutureToAkka(f).map {
      case _ => CreateEntityResult(op)
    }
  }

  val preparedGetEntity = session.prepare(
    s"""
       |SELECT shard_id, entity_key, created_at FROM $entitiesTableName
       |WHERE shard_id = ? AND entity_key = ?
     """.stripMargin)

  def getEntity(op: GetEntity): Future[GetEntityResult] = {
    val shardId: java.lang.Integer = op.shardId
    val entityKey: String = op.entityKey
    val f = session.executeAsync(new BoundStatement(preparedGetEntity).bind(shardId, entityKey))
    guavaFutureToAkka(f).map { resultSet =>
      val row = resultSet.one()
      if (row != null) {
        val shardId = row.getInt(0)
        val entityKey = row.getString(1)
        GetEntityResult(op, shardId, entityKey)
      } else throw ApiException(ResourceNotFound)
    }
  }

  val preparedDeleteEntity = session.prepare(
    s"""
       |DELETE FROM $entitiesTableName
       |WHERE shard_id = ? AND entity_key = ?
     """.stripMargin)

  def deleteEntity(op: DeleteEntity): Future[DeleteEntityResult] = {
    val shardId: java.lang.Integer = op.shardId
    val entityKey: String = op.entityKey
    val f = session.executeAsync(new BoundStatement(preparedDeleteEntity).bind(shardId, entityKey))
    guavaFutureToAkka(f).map {
      case _ => DeleteEntityResult(op)
    }
  }

  val preparedListEntities = session.prepare(
    s"""
       |SELECT shard_id, entity_key, created_at FROM $entitiesTableName
       |WHERE shard_id = ? AND entity_key > ?
       |ORDER BY entity_key ASC
       |LIMIT ?
     """.stripMargin)

  def listEntities(op: ListEntities): Future[ListEntitiesResult] = {
    val shardId: java.lang.Integer = op.shardId
    val entityKey: java.lang.String = op.token.map(_.entityKey).getOrElse("")
    val limit: java.lang.Integer = op.limit
    val f = session.executeAsync(new BoundStatement(preparedListEntities).bind(shardId, entityKey, limit))
    guavaFutureToAkka(f).map { resultSet =>
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
    val f = session.executeAsync(s"TRUNCATE $entitiesTableName")
    guavaFutureToAkka(f).map { resultSet => }
  }
}