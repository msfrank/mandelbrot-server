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
import io.mandelbrot.core.{BadRequest, ApiException, ServiceExtension}
import io.mandelbrot.core.entity.EntityFunctions.{ShardResolver, KeyExtractor, PropsCreator}
import scala.collection.mutable
import java.net.URI

import io.mandelbrot.core.registry.RegistryServiceOperation
import io.mandelbrot.core.system.{ProbeSystem, ProbeSystemOperation, ProbeOperation}

/**
 *
 */
class StandaloneEntityManager(settings: ClusterSettings,
                              shardResolver: ShardResolver,
                              keyExtractor: KeyExtractor,
                              propsCreator: PropsCreator) extends Actor with ActorLogging {

  // config
  val defaultAttempts = 3

  val coordinator = {
    val props = ServiceExtension.makePluginProps(settings.coordinator.plugin, settings.coordinator.settings)
    log.info("loading coordinator plugin {}", settings.coordinator.plugin)
    context.actorOf(props, "coordinator")
  }

  val shardManager = context.actorOf(ShardManager.props(context.parent, propsCreator,
    ShardManager.StandaloneAddress, settings.totalShards), "entity-manager")

  log.info("initializing standalone mode")

  override def preStart(): Unit = {
    context.actorOf(ShardBalancer.props(context.parent, self,
      Map(ShardManager.StandaloneAddress -> shardManager.path), settings.totalShards),
      "shard-balancer")
  }

  def receive = {

    case result: ShardBalancerResult =>
      log.debug("shard balancer completed")

    // forward any messages for the coordinator
    case op: EntityServiceOperation =>
      coordinator forward op

    // send envelopes directly to the shard manager
    case envelope: EntityEnvelope =>
      shardManager ! envelope

    // we assume any other message is for an entity, so we wrap it in an envelope
    case message: Any =>
      try {
        val shardKey = shardResolver(message)
        val entityKey = keyExtractor(message)
        shardManager ! EntityEnvelope(sender(), message, shardKey, entityKey, attempts = defaultAttempts)
      } catch {
        case ex: Throwable =>
          sender() ! EntityDeliveryFailed(message, new ApiException(BadRequest))
      }

  }
}
