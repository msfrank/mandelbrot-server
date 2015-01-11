/**
 * Copyright 2014 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Mandelbrot.
 *
 * Mandelbrot is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Mandelbrot is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Mandelbrot.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mandelbrot.core.cluster

import akka.actor._
import scala.concurrent.duration.FiniteDuration

/**
 * The PutShardTask is invoked by the ShardBalancer to place a shard at the specified
 * address.
 */
class PutShardTask(op: PutShard, services: ActorRef, monitor: ActorRef, timeout: FiniteDuration) extends Actor with ActorLogging {
  import PutShardTask.TaskTimeout
  import context.dispatcher

  // config
  val shardId = op.shardId
  val targetNode = op.targetNode

  // state
  val cancellable = context.system.scheduler.scheduleOnce(timeout, self, TaskTimeout)

  services ! GetShard(shardId)

  def receive = {

    case result: GetShardResult =>
      // prepare targetNode to receive shard
      log.debug("preparing {} to receive shard {}", targetNode, result.shardId)
      context.actorSelection(targetNode) ! PrepareShard(shardId)

    case result: PrepareShardResult =>
      // write new shard owner
      log.debug("shard {} now assigned to {}", result.op.shardId, sender().path)
      services ! UpdateShard(shardId, targetNode.address)

    case result: UpdateShardResult =>
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

    case TaskTimeout =>
      monitor ! PutShardFailed(op, new Exception("timed out while performing task %s".format(op)))
      context.stop(self)
  }

  override def postStop(): Unit = cancellable.cancel()
}

object PutShardTask {
  def props(op: PutShard, services: ActorRef, monitor: ActorRef, timeout: FiniteDuration) = {
    Props(classOf[PutShardTask], op, services, monitor, timeout)
  }
  case object TaskTimeout
}

case class PutShard(shardId: Int, targetNode: ActorPath)
case class PutShardComplete(op: PutShard)
case class PutShardFailed(op: PutShard, ex: Throwable)
