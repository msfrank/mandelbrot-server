package io.mandelbrot.persistence.cassandra.dal

import akka.actor.AddressFromURIString
import com.datastax.driver.core.{BoundStatement, Session}
import io.mandelbrot.core.entity._
import io.mandelbrot.core.{ApiException, Conflict, ResourceNotFound}
import io.mandelbrot.persistence.cassandra.CassandraEntityCoordinatorSettings
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}

/**
 *
 */
class ShardsDAL(settings: CassandraEntityCoordinatorSettings,
                val session: Session,
                implicit val ec: ExecutionContext) extends AbstractDriver {

  val tableName = "shards"

  // ensure that shards table exists
  session.execute(
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  p int,
       |  shard_id int,
       |  address text,
       |  last_update timestamp,
       |  PRIMARY KEY (p, shard_id)
       |);
     """.stripMargin)

  private val preparedCreateShard = session.prepare(
    s"""
       |INSERT INTO $tableName (p, shard_id, address, last_update)
       |VALUES (0, ?, ?, ?)
       |IF NOT EXISTS
     """.stripMargin)

  def createShard(op: CreateShard, timestamp: DateTime): Future[CreateShardResult] = {
    val shardId: java.lang.Integer = op.shardId
    val address: String = op.address.toString
    executeAsync(new BoundStatement(preparedCreateShard).bind(shardId, address, timestamp.toDate)).map {
      case resultSet if !resultSet.wasApplied() => throw ApiException(Conflict)
      case _ => CreateShardResult(op)
    }
  }

  private val preparedUpdateShard = session.prepare(
    s"""
       |UPDATE $tableName
       |SET address = ?, last_update = ?
       |WHERE p = 0 AND shard_id = ?
       |IF address = ?
     """.stripMargin)

  def updateShard(op: UpdateShard, timestamp: DateTime): Future[UpdateShardResult] = {
    val shardId: java.lang.Integer = op.shardId
    val address: String = op.address.toString
    val prev: String = op.prev.toString
    executeAsync(new BoundStatement(preparedUpdateShard).bind(address, timestamp.toDate, shardId, prev)).map {
      case resultSet if !resultSet.wasApplied() => throw ApiException(Conflict)
      case _ => UpdateShardResult(op)
    }
  }

  private val preparedGetShard = session.prepare(
    s"""
       |SELECT shard_id, address, last_update FROM $tableName
       |WHERE p = 0 AND shard_id = ?
     """.stripMargin)

  def getShard(op: GetShard): Future[GetShardResult] = {
    val shardId: java.lang.Integer = op.shardId
    executeAsync(new BoundStatement(preparedGetShard).bind(shardId)).map { resultSet =>
      val row = resultSet.one()
      if (row != null) {
        val shardId = row.getInt(0)
        val address = AddressFromURIString(row.getString(1))
        val lastUpdate = new DateTime(row.getDate(2))
        GetShardResult(op, shardId, address)
      } else throw ApiException(ResourceNotFound)
    }
  }

  private val preparedListShards = session.prepare(
    s"""
       |SELECT shard_id, address, last_update FROM $tableName
       |WHERE p = 0 AND shard_id >= ?
       |ORDER BY shard_id ASC
       |LIMIT ?
     """.stripMargin)

  def listShards(op: ListShards): Future[ListShardsResult] = {
    val shardId: java.lang.Integer = op.token.map(1 + _.shardId: java.lang.Integer).getOrElse(0)
    val limit: java.lang.Integer = op.limit
    executeAsync(new BoundStatement(preparedListShards).bind(shardId, limit)).map { resultSet =>
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
    executeAsync(s"TRUNCATE $tableName").map { resultSet => }
  }
}