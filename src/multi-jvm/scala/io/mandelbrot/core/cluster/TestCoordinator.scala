package io.mandelbrot.core.cluster

import akka.actor._
import com.typesafe.config.Config

class TestCoordinator(shards: ShardRing) extends Actor with ActorLogging with Coordinator with Stash {

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

    case op: GetShard =>
      log.debug("{} requests shard {}", sender().path, op.shardKey)
      shards(op.shardKey) match {
        case Some((shardId, address)) =>
          val (width,_) = shards.get(shardId).get
          sender() ! GetShardResult(op, shardId, width, address)
        case None =>
      }

    case op: UpdateMemberShards =>
      log.debug("{} updates shards", sender().path)
  }
}

object TestCoordinator {
  def props(shards: ShardRing) = Props(classOf[TestCoordinator], shards)
  def settings(config: Config): Option[Any] = None
}
