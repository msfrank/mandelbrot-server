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

package io.mandelbrot.core.entity

import akka.actor._
import io.mandelbrot.core.{RetryLater, ApiException}
import scala.concurrent.duration.FiniteDuration

/**
 * The PutShardTask is invoked by the BalancerTask to place a shard at the specified
 * address.
 */
class PutShardTask(op: PutShard,
                   services: ActorRef,
                   monitor: ActorRef,
                   timeout: FiniteDuration) extends Actor with ActorLogging {
  import PutShardTask.TaskTimeout
  import context.dispatcher

  // config
  val shardId = op.shardId
  val address = op.address
  val path = op.path

  // state
  val cancellable = context.system.scheduler.scheduleOnce(timeout, self, TaskTimeout)

  // prepare targetNode to receive shard
  log.debug("preparing {} to receive shard {}", path, op.shardId)
  context.actorSelection(path) ! PrepareShard(shardId)

  def receive = {

    case result: PrepareShardResult =>
      // write new shard owner
      op.prev match {
        case Some(prev) =>
          log.debug("shard {} now assigned to {} (was {}", result.op.shardId, sender().path, prev)
          services ! UpdateShard(shardId, address, prev)
        case None =>
          log.debug("shard {} now assigned to {}", result.op.shardId, sender().path)
          services ! CreateShard(shardId, address)
      }

    case result: CreateShardResult =>
      // tell targetNode to recover shard
      log.debug("notifying {} to recover shard {}", path, result.op.shardId)
      context.actorSelection(path) ! RecoverShard(shardId)

    case result: UpdateShardResult =>
      // tell targetNode to recover shard
      log.debug("notifying {} to recover shard {}", path, result.op.shardId)
      context.actorSelection(path) ! RecoverShard(shardId)

    case result: RecoverShardResult =>
      log.debug("{} acknowledges receipt of shard {}", sender().path, result.op.shardId)
      monitor ! PutShardComplete(op)
      context.stop(self)

    case failure: ShardBalancerOperationFailed =>
      log.debug("failed to put shard {}: {}", shardId, failure)
      monitor ! PutShardFailed(op, failure.failure)
      context.stop(self)

    case failure: EntityServiceOperationFailed =>
      log.debug("failed to put shard {}: {}", shardId, failure)
      monitor ! PutShardFailed(op, failure.failure)
      context.stop(self)

    case TaskTimeout =>
      monitor ! PutShardFailed(op, ApiException(RetryLater))
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

case class PutShard(shardId: Int, address: Address, path: ActorPath, prev: Option[Address])
case class PutShardComplete(op: PutShard)
case class PutShardFailed(op: PutShard, ex: Throwable)
