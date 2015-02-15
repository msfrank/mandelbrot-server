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

import akka.cluster.Cluster
import akka.actor._
import akka.contrib.pattern.DistributedPubSubMediator

import io.mandelbrot.core._
import io.mandelbrot.core.entity.EntityFunctions.PropsCreator

/**
 * 
 */
class ClusterEntityManager(settings: ClusterSettings, propsCreator: PropsCreator) extends Actor with ActorLogging {

  // config
  val selfAddress = Cluster(context.system).selfAddress

  // state
  val coordinator = {
    val props = ServiceExtension.makePluginProps(settings.coordinator.plugin, settings.coordinator.settings)
    log.info("loading coordinator plugin {}", settings.coordinator.plugin)
    context.actorOf(props, "coordinator")
  }
  var clusterMonitor = ActorRef.noSender
  var gossiper = ActorRef.noSender
  var shardManager = ActorRef.noSender
  var shardBalancer = ActorRef.noSender

  log.info("initializing cluster mode")

  // listen for ClusterUp and ClusterDown
  context.system.eventStream.subscribe(self, classOf[ClusterMonitorEvent])

  // we try to join immediately if seed nodes are specified
  override def preStart(): Unit = {
    if (settings.seedNodes.nonEmpty) {
      Cluster(context.system).joinSeedNodes(settings.seedNodes.map(AddressFromURIString(_)).toSeq)
      log.info("joining cluster using seed nodes {}", settings.seedNodes.mkString(","))
      context.become(down)
    } else log.info("waiting for seed nodes")
  }

  def receive = waiting

  // wait for cluster join message containing seed nodes
  def waiting: Receive = {

    case op: JoinCluster =>
      val seedNodes = op.seedNodes.map(AddressFromURIString(_)).toSeq
      Cluster(context.system).joinSeedNodes(seedNodes)
      log.debug("joining cluster using seed nodes {}", seedNodes.mkString(","))
      context.become(down)

    // tell client to try later
    case op: EntityServiceOperation =>
      sender() ! EntityServiceOperationFailed(op, ApiException(RetryLater))

    // tell client to try later
    case envelope: EntityEnvelope =>
      sender() ! EntityDeliveryFailed(envelope.op, ApiException(RetryLater))
  }

  // cluster is UP, we can service messages from clients
  def up: Receive = {

    // cluster monitor emits this message
    case event: ClusterUp =>

    // cluster monitor emits this message
    case event: ClusterDown =>
      log.debug("cluster becomes DOWN")
      context.become(down)

    // forward any messages for the coordinator
    case op: EntityServiceOperation =>
      coordinator forward op

    // send envelopes to the shard manager
    case envelope: EntityEnvelope =>
      shardManager ! envelope
  }

  // cluster is down, we just tell client to try again later
  def down: Receive = {

    // cluster monitor emits this message
    case event: ClusterUp =>
      // create actors if they don't exist.  this should happen only once.
      if (clusterMonitor.equals(ActorRef.noSender))
        clusterMonitor = context.actorOf(ClusterMonitor.props(settings.minNrMembers), "cluster-monitor")
      if (gossiper.equals(ActorRef.noSender))
        gossiper = context.actorOf(DistributedPubSubMediator.props(None), "cluster-gossiper")
      if (shardManager.equals(ActorRef.noSender))
        shardManager = context.actorOf(ShardManager.props(context.parent, propsCreator, selfAddress, settings.totalShards, gossiper), "entity-manager")
      if (shardBalancer.equals(ActorRef.noSender))
        shardBalancer = context.actorOf(ClusterBalancer.props(settings, context.parent, shardManager.path.elements), "shard-balancer")
      log.debug("cluster becomes UP")
      context.become(up)

    // cluster monitor emits this message
    case event: ClusterDown =>

    // tell client to try later
    case op: EntityServiceOperation =>
      sender() ! EntityServiceOperationFailed(op, ApiException(RetryLater))

    // tell client to try later
    case envelope: EntityEnvelope =>
      sender() ! EntityDeliveryFailed(envelope.op, ApiException(RetryLater))
  }
}
