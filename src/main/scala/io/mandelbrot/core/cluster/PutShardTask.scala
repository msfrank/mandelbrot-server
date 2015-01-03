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

  coordinator ! GetShard(shardId)

  def receive = {

    case result: GetShardResult =>
      // prepare targetNode to receive shard
      context.actorSelection(targetNode) ! PrepareShard(shardId)

    case result: PrepareShardResult =>
      // write new shard owner
      coordinator ! CommitShard(shardId, targetNode.address)

    case result: CommitShardResult =>
      // tell targetNode to recover shard
      context.actorSelection(targetNode) ! RecoverShard(shardId)

    case result: RecoverShardResult =>
      monitor ! PutShardComplete(op)
      context.stop(self)

    case failure: ClusterServiceOperationFailed =>
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
