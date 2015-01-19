package io.mandelbrot.core.cluster

import akka.actor._
import com.typesafe.config.Config
import io.mandelbrot.core.{Conflict, ResourceNotFound, ApiException}

case class TestCoordinatorSettings(shardMap: ShardMap, masterAddress: Address, selfAddress: Address)

class TestCoordinator(settings: TestCoordinatorSettings) extends Actor with ActorLogging with Coordinator {

  val shardMap = settings.shardMap
  val masterAddress = settings.masterAddress
  val selfAddress = settings.selfAddress

  def receive = {

    case message if !selfAddress.equals(masterAddress) =>
      val actorPath = RootActorPath(masterAddress) / self.path.elements
      log.debug("forwarding message {} to master {}", message, actorPath)
      context.actorSelection(actorPath) forward message

    case op: ListShards =>
      log.debug("{} requests all shards", sender().path)
      val shards = shardMap.assigned.map(entry => Shard(entry.shardId, entry.width, entry.address))
      sender() ! ListShardsResult(op, shards)

    case op: GetShard =>
      log.debug("{} requests shard {}:{}", sender().path, op.shardId, op.width)
      shardMap.get(op.shardId) match {
        case shard: AssignedShardEntry if shard.width == op.width =>
          sender() ! GetShardResult(op, shard.shardId, shard.width, Some(shard.address))
        case shard: ShardEntry =>
          sender() ! ClusterServiceOperationFailed(op, ApiException(ResourceNotFound))
      }

    case op: FindShard =>
      log.debug("{} requests shard for key {}", sender().path, op.shardKey)
      shardMap(op.shardKey) match {
        case shard: AssignedShardEntry =>
          sender() ! FindShardResult(op, shard.shardId, shard.width, Some(shard.address))
        case shard: ShardEntry =>
          sender() ! ClusterServiceOperationFailed(op, ApiException(ResourceNotFound))
      }

    case op: CreateShard =>
      shardMap.get(op.shardId) match {
        case entry: MissingShardEntry =>
          log.debug("{} creates shard {} and assigns to {}", sender().path, op.shardId, op.address)
          shardMap.assign(op.shardId, op.address)
          sender() ! CreateShardResult(op)
        case entry: ShardEntry =>
          sender() ! ClusterServiceOperationFailed(op, ApiException(Conflict))
      }

    case op: UpdateShard =>
      shardMap.get(op.shardId) match {
        case entry: MissingShardEntry =>
          sender() ! ClusterServiceOperationFailed(op, ApiException(ResourceNotFound))
        case entry: ShardEntry =>
          log.debug("{} updates shard {} from {} to {}", sender().path, op.shardId, op.address, op.prev)
          shardMap.assign(op.shardId, op.address)
          sender() ! UpdateShardResult(op)
      }
  }
}

object TestCoordinator {
  def props(settings: TestCoordinatorSettings) = {
    Props(classOf[TestCoordinator], settings)
  }
  def settings(config: Config): Option[Any] = None
}
