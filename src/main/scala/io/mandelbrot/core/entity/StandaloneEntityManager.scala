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
import io.mandelbrot.core._
import io.mandelbrot.core.entity.EntityFunctions.{ShardResolver, KeyExtractor, PropsCreator}
import scala.collection.mutable
import java.net.URI

import io.mandelbrot.core.registry.RegistryServiceOperation
import io.mandelbrot.core.system.{ProbeSystem, ProbeSystemOperation, ProbeOperation}

/**
 *
 */
class StandaloneEntityManager(settings: ClusterSettings, propsCreator: PropsCreator) extends Actor with ActorLogging {

  val coordinator = {
    val props = ServiceExtension.makePluginProps(settings.coordinator.plugin, settings.coordinator.settings)
    log.info("loading coordinator plugin {}", settings.coordinator.plugin)
    context.actorOf(props, "coordinator")
  }

  val shardManager = context.actorOf(ShardManager.props(context.parent, propsCreator,
    ShardManager.StandaloneAddress, settings.totalShards, ActorRef.noSender), "entity-manager")

  log.info("initializing standalone mode")

  override def preStart(): Unit = {
    context.actorOf(BalancerTask.props(context.parent, self,
      Map(ShardManager.StandaloneAddress -> shardManager.path), settings.totalShards),
      "shard-balancer")
  }

  def receive = {

    case result: BalancerComplete =>
      log.debug("shard balancer completed")

    // send envelopes to the shard manager
    case envelope: EntityEnvelope =>
      shardManager ! envelope

    // return the current status for the specified node
    case op: GetNodeStatus =>
      sender() ! EntityServiceOperationFailed(op, ApiException(NotImplemented))

    // return the current cluster status
    case op: GetClusterStatus =>
      sender() ! EntityServiceOperationFailed(op, ApiException(NotImplemented))

    // return the current shard map status
    case op: GetShardMapStatus =>
      shardManager forward op

    // forward any other operations for the coordinator
    case op: EntityServiceOperation =>
      coordinator forward op
  }
}
