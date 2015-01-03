package io.mandelbrot.core.cluster

import akka.actor._
import com.typesafe.config.Config

class TestCoordinator(shards: ShardMap) extends Actor with ActorLogging with Coordinator with Stash {

  def receive = {

    case op: GetAllShards =>
      log.debug("{} requests all shards", sender().path)
      sender() ! GetAllShardsResult(op, shards.assigned)

    case op: GetShard =>
      log.debug("{} requests shard for key {}", sender().path, op.shardKey)
      shards(op.shardKey) match {
        case Some((shardId, address)) =>
          sender() ! GetShardResult(op, shardId, Some(address))
        case None =>
          sender() ! GetShardResult(op, op.shardKey, None)
      }

    case op: CommitShard =>
      log.debug("{} commits {} to shard {}", sender().path, op.target, op.shardId)
      shards.put(op.shardId, op.target)
      sender() ! CommitShardResult(op)
  }
}

object TestCoordinator {
  def props(shards: ShardMap) = Props(classOf[TestCoordinator], shards)
  def settings(config: Config): Option[Any] = None
}
