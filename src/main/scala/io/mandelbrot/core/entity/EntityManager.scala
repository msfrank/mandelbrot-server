package io.mandelbrot.core.entity

import akka.actor.{Address, Props}
import akka.cluster.{UniqueAddress, MemberStatus}

import io.mandelbrot.core.{ServiceQuery, ServiceCommand, ServiceOperationFailed, ServiceOperation}
import io.mandelbrot.core.entity.EntityFunctions.PropsCreator

/**
 *
 */
object EntityManager {
  def props(settings: ClusterSettings, propsCreator: PropsCreator) = {
    if (settings.enabled)
      Props(classOf[ClusterEntityManager], settings, propsCreator)
    else
      Props(classOf[StandaloneEntityManager], settings, propsCreator)
  }
}

case class Shard(shardId: Int, address: Address)
case class Entity(shardId: Int, entityKey: String)
case class NodeStatus(address: Address, uid: Int, status: MemberStatus, roles: Set[String])
case class ClusterStatus(nodes: Vector[NodeStatus], leader: Option[Address], unreachable: Set[Address])
case class ShardMapStatus(shards: Set[ShardEntry], totalShards: Int)

sealed trait EntityServiceOperation extends ServiceOperation
sealed trait EntityServiceCommand extends ServiceCommand with EntityServiceOperation
sealed trait EntityServiceQuery extends ServiceQuery with EntityServiceOperation
case class EntityServiceOperationFailed(op: EntityServiceOperation, failure: Throwable) extends ServiceOperationFailed

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

case class GetNodeStatus(node: Option[Address]) extends EntityServiceQuery
case class GetNodeStatusResult(op: GetNodeStatus, status: NodeStatus)

case class GetClusterStatus() extends EntityServiceQuery
case class GetClusterStatusResult(op: GetClusterStatus, status: ClusterStatus)

case class GetShardMapStatus() extends EntityServiceQuery
case class GetShardMapStatusResult(op: GetShardMapStatus, status: ShardMapStatus)

/* marker trait for Coordinator implementations */
trait Coordinator
