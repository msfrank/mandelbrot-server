package io.mandelbrot.core.cluster

import akka.actor._
import scala.concurrent.duration.FiniteDuration

/**
 *
 */
class PutShardTask(op: PutShard, coordinator: ActorRef, monitor: ActorRef, timeout: FiniteDuration) extends Actor with ActorLogging {
  import context.dispatcher

  // config
  val shardId = op.shardId
  val targetNode = op.targetNode

  // state
  val cancellable = context.system.scheduler.scheduleOnce(timeout, self, ShardTaskTimeout)

  context.actorSelection(targetNode) ! Identify(shardId)

  coordinator ! GetShard(shardId)

  def receive = {

    case result: GetShardResult =>
      // prepare targetNode to receive shard
      log.debug("preparing {} to receive shard {}", targetNode, result.shardId)
      context.actorSelection(targetNode) ! PrepareShard(shardId)

    case result: PrepareShardResult =>
      // write new shard owner
      log.debug("shard {} now assigned to {}", result.op.shardId, sender().path)
      coordinator ! CommitShard(shardId, targetNode.address)

    case result: CommitShardResult =>
      // tell targetNode to recover shard
      log.debug("notifying {} to recover shard {}", targetNode, result.op.shardId)
      context.actorSelection(targetNode) ! RecoverShard(shardId)

    case result: RecoverShardResult =>
      log.debug("{} acknowledges receipt of shard {}", sender().path, result.op.shardId)
      monitor ! PutShardComplete(op)
      context.stop(self)

    case failure: ClusterServiceOperationFailed =>
      log.debug("failed to put shard {}: {}", shardId, failure)
      monitor ! PutShardFailed(op, failure.failure)
      context.stop(self)

    case ShardTaskTimeout =>
      monitor ! PutShardFailed(op, new Exception("timed out while performing task %s".format(op)))
      context.stop(self)
  }

  override def postStop(): Unit = cancellable.cancel()
}

object PutShardTask {
  def props(op: PutShard, coordinator: ActorRef, monitor: ActorRef, timeout: FiniteDuration) = {
    Props(classOf[PutShardTask], op, coordinator, monitor, timeout)
  }
}

case class PutShard(shardId: Int, targetNode: ActorPath)
case class PutShardComplete(op: PutShard)
case class PutShardFailed(op: PutShard, ex: Throwable)
