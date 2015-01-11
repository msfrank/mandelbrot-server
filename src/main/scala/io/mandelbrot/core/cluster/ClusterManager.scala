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

import akka.cluster.Cluster
import akka.actor._

import io.mandelbrot.core.{BadRequest, ApiException, ServiceExtension}
import io.mandelbrot.core.cluster.EntityFunctions.{ShardResolver, KeyExtractor, PropsCreator}

/**
 * 
 */
class ClusterManager(settings: ClusterSettings,
                     shardResolver: ShardResolver,
                     keyExtractor: KeyExtractor,
                     propsCreator: PropsCreator) extends Actor with ActorLogging {

  // config
  val selfAddress = Cluster(context.system).selfAddress
  val defaultAttempts = 3

  // state
  var incubating = true
  var running = false
  var status: ClusterMonitorEvent = ClusterUnknown
  var shardBalancer: Option[ActorRef] = None

  val coordinator = {
    val props = ServiceExtension.makePluginProps(settings.coordinator.plugin, settings.coordinator.settings)
    log.info("loading coordinator plugin {}", settings.coordinator.plugin)
    context.actorOf(props, "coordinator")
  }

  val clusterMonitor = context.actorOf(ClusterMonitor.props(settings.minNrMembers), "cluster-monitor")
  val entityManager = context.actorOf(EntityManager.props(context.parent,
    shardResolver, keyExtractor, propsCreator, selfAddress, settings.totalShards, settings.initialWidth),
    "entity-manager")

  log.info("initializing cluster mode")

  override def preStart(): Unit = {
    if (settings.seedNodes.nonEmpty) {
      Cluster(context.system).joinSeedNodes(settings.seedNodes.map(AddressFromURIString(_)).toSeq)
      self ! JoinCluster(settings.seedNodes.toVector)
      log.info("joining cluster using seed nodes {}", settings.seedNodes.mkString(","))
    } else
      log.info("waiting for seed nodes")
  }

  def receive = {

    // try to join the cluster using the specified seed nodes
    case op: JoinCluster =>
      if (incubating) {
        val seedNodes = op.seedNodes.map(AddressFromURIString(_)).toSeq
        Cluster(context.system).joinSeedNodes(seedNodes)
        incubating = false
        log.debug("attempting to join cluster")
      } else log.debug("ignoring join request, we are not incubating")

    // cluster monitor emits this message
    case event: ClusterUp =>
      log.debug("cluster is up")
      running = true
      status = event
      if (event.leader.equals(selfAddress) && shardBalancer.isEmpty) {
      } else if (!event.leader.equals(selfAddress) && shardBalancer.nonEmpty) {
      }

    // cluster monitor emits this message
    case event: ClusterDown =>
      log.debug("cluster is down")
      running = false
      status = event
      if (shardBalancer.nonEmpty) {
      }

    // forward any messages for the coordinator
    case op: ClusterServiceOperation =>
      coordinator forward op

    // send envelopes directly to the entity manager
    case envelope: EntityEnvelope =>
      entityManager ! envelope

    // we assume any other message is for an entity, so we wrap it in an envelope
    case message: Any =>
      entityManager ! EntityEnvelope(sender(), message, attempts = defaultAttempts)
  }
}

object ClusterManager {
  def props(settings: ClusterSettings, shardResolver: ShardResolver, keyExtractor: KeyExtractor, propsCreator: PropsCreator) = {
    Props(classOf[ClusterManager], settings, shardResolver, keyExtractor, propsCreator)
  }
}

case class JoinCluster(seedNodes: Vector[String])

case class Shard(shardId: Int, width: Int, address: Address)

sealed trait ClusterServiceOperation
sealed trait ClusterServiceCommand extends ClusterServiceOperation
sealed trait ClusterServiceQuery extends ClusterServiceOperation
case class ClusterServiceOperationFailed(op: ClusterServiceOperation, failure: Throwable)

case class ListShards() extends ClusterServiceQuery
case class ListShardsResult(op: ListShards, shards: Vector[Shard])

case class GetShard(shardKey: Int) extends ClusterServiceQuery
case class GetShardResult(op: GetShard, shardId: Int, width: Int, address: Option[Address])

case class UpdateShard(shardId: Int, target: Address) extends ClusterServiceCommand
case class UpdateShardResult(op: UpdateShard)

/* marker trait for Coordinator implementations */
trait Coordinator
