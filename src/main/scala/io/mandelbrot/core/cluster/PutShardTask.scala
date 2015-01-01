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

  // prepare targetNode to receive shard
  context.actorSelection(targetNode) ! PrepareShard(shardId)

  def receive = {

    case result: PrepareShardResult =>
      if (result.accepted) {
      } else {
        monitor ! PutShardFailed(op, new Exception("%s did not accept shard %d".format(sender().path, shardId)))
        context.stop(self)
      }

    case ShardTaskTimeout =>
      monitor ! PutShardFailed(op, new Exception("timed out while performing task %s".format(op)))
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
