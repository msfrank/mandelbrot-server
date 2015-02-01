package io.mandelbrot.core.entity

import akka.actor.{Address, Props}

import io.mandelbrot.core.entity.EntityFunctions.{PropsCreator, KeyExtractor, ShardResolver}

/**
 *
 */
object EntityManager {
  def props(settings: ClusterSettings, shardResolver: ShardResolver, keyExtractor: KeyExtractor, propsCreator: PropsCreator) = {
    if (settings.enabled)
      Props(classOf[ClusterEntityManager], settings, shardResolver, keyExtractor, propsCreator)
    else
      Props(classOf[StandaloneEntityManager], settings, shardResolver, keyExtractor, propsCreator)
  }
}

case class JoinCluster(seedNodes: Vector[String])

case class Shard(shardId: Int, address: Address)
case class Entity(shardId: Int, entityKey: String)

sealed trait EntityServiceOperation
sealed trait EntityServiceCommand extends EntityServiceOperation
sealed trait EntityServiceQuery extends EntityServiceOperation
case class ClusterServiceOperationFailed(op: EntityServiceOperation, failure: Throwable)

case class ListShards(limit: Int, token: Option[Shard]) extends EntityServiceQuery
case class ListShardsResult(op: ListShards, shards: Vector[Shard], token: Option[Shard])

case class GetShard(shardId: Int) extends EntityServiceQuery
case class GetShardResult(op: GetShard, shardId: Int, address: Address)

case class CreateShard(shardId: Int, address: Address) extends EntityServiceCommand
case class CreateShardResult(op: CreateShard)

case class UpdateShard(shardId: Int, address: Address, prev: Address) extends EntityServiceCommand
case class UpdateShardResult(op: UpdateShard)

case class ListEntities(shardId: Int, limit: Int, token: Option[Entity]) extends EntityServiceQuery
case class ListEntitiesResult(op: ListEntities, entities: Vector[Entity], token: Option[Entity])

case class GetEntity(shardId: Int, entityKey: String) extends EntityServiceQuery
case class GetEntityResult(op: GetEntity, shardId: Int, entityKey: String)

case class CreateEntity(shardId: Int, entityKey: String) extends EntityServiceCommand
case class CreateEntityResult(op: CreateEntity)

case class DeleteEntity(shardId: Int, entityKey: String) extends EntityServiceCommand
case class DeleteEntityResult(op: DeleteEntity)

/* marker trait for Coordinator implementations */
trait Coordinator