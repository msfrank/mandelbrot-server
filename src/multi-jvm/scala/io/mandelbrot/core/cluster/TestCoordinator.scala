package io.mandelbrot.core.cluster

import akka.actor._
import com.typesafe.config.Config

class TestCoordinator(shardMap: ShardMap) extends Actor with ActorLogging with Coordinator {

  def receive = {

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
  def props(shards: ShardMap) = Props(classOf[TestCoordinator], shards)
  def settings(config: Config): Option[Any] = None
}

class ClusterTestCoordinator(master: Address, props: Props) extends Actor with ActorLogging with Coordinator {

  def receive = {
    case message =>

  }
}