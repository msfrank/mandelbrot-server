package io.mandelbrot.core.cluster

import akka.actor._
import com.typesafe.config.Config

class TestCoordinator(shards: ShardMap) extends Actor with ActorLogging with Coordinator with Stash {

  // state
  var running = false

  context.system.eventStream.subscribe(self, classOf[ClusterUp])

  def receive = {

    case op: ClusterUp =>
      if (!running) {
        running = true
        unstashAll()
        log.debug("unstashing messages")
      }

    case any if !running =>
      stash()

    case op: GetAllShards =>
      log.debug("{} requests all shards", sender().path)
      sender() ! GetAllShardsResult(op, shards.assigned)

    case op: GetShard =>
      log.debug("{} requests shard {}", sender().path, op.shardKey)
      shards(op.shardKey) match {
        case Some((shardId, address)) =>
          sender() ! GetShardResult(op, shardId, address)
        case None =>
      }
  }
}

object TestCoordinator {
  def props(shards: ShardMap) = Props(classOf[TestCoordinator], shards)
  def settings(config: Config): Option[Any] = None
}
