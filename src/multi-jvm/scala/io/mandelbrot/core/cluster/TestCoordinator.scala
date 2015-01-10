package io.mandelbrot.core.cluster

import akka.actor._
import com.typesafe.config.Config

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

    case op: GetAllShards =>
      log.debug("{} requests all shards", sender().path)
      val shards = shardMap.assigned.map(entry => Shard(entry.shardId, entry.width, entry.address))
      sender() ! GetAllShardsResult(op, shards)

    case op: GetShard =>
      log.debug("{} requests shard for key {}", sender().path, op.shardKey)
      shardMap(op.shardKey) match {
        case shard: AssignedShardEntry =>
          sender() ! GetShardResult(op, shard.shardId, shard.width, Some(shard.address))
        case shard: ShardEntry =>
          sender() ! GetShardResult(op, shard.shardId, shard.width, None)
      }

    case op: CommitShard =>
      log.debug("{} commits {} to shard {}", sender().path, op.target, op.shardId)
      shardMap.assign(op.shardId, op.target)
      sender() ! CommitShardResult(op)
  }
}

object TestCoordinator {
  def props(settings: TestCoordinatorSettings) = {
    Props(classOf[TestCoordinator], settings)
  }
  def settings(config: Config): Option[Any] = None
}
