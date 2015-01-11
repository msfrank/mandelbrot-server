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
import akka.pattern._
import scala.concurrent.duration._

/**
 * LookupShardTask queries the coordinator for the current address mapped to
 * the specified shard.  If there is no address, then the task will continue
 * querying at regular intervals until the shard has been assigned.
 */
class LookupShardTask(op: LookupShard,
                      coordinator: ActorRef,
                      monitor: ActorRef,
                      timeout: FiniteDuration) extends Actor with ActorLogging {
  import LookupShardTask.PerformQuery
  import context.dispatcher

  // config
  val delay = 5.seconds

  log.debug("attempting to resolve shard for key {}", op.shardKey)

  override def preStart(): Unit = self ! PerformQuery
  
  def receive = {

    case PerformQuery =>
      coordinator.ask(GetShard(op.shardKey))(timeout).pipeTo(self)

    case result: GetShardResult if result.address.isEmpty =>
      context.system.scheduler.scheduleOnce(delay, self, PerformQuery)
      
    case GetShardResult(_, shardId, width, Some(address)) =>
      context.stop(self)
      monitor ! LookupShardResult(op, shardId, width, address)
      
    case failure: ClusterServiceOperationFailed =>
      context.system.scheduler.scheduleOnce(delay, self, PerformQuery)

    case failure: AskTimeoutException =>
      context.system.scheduler.scheduleOnce(delay, self, PerformQuery)
  }
}

object LookupShardTask {
  def props(op: LookupShard, coordinator: ActorRef, monitor: ActorRef, timeout: FiniteDuration) = {
    Props(classOf[LookupShardTask], op, coordinator, monitor, timeout)
  }
  case object PerformQuery
}

case class LookupShard(shardKey: Int)
case class LookupShardResult(op: LookupShard, shardId: Int, width: Int, address: Address)
