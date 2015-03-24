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
import akka.cluster.{UniqueAddress, Cluster}
import akka.contrib.pattern.DistributedPubSubMediator

import io.mandelbrot.core._
import io.mandelbrot.core.entity.EntityFunctions.{EntityReviver, PropsCreator}

/**
 * 
 */
class ClusterEntityManager(settings: ClusterSettings,
                           propsCreator: PropsCreator,
                           entityReviver: EntityReviver) extends Actor with ActorLogging {

  // config
  val selfAddress: Address = Cluster(context.system).selfAddress
  val selfUniqueAddress: UniqueAddress = Cluster(context.system).selfUniqueAddress

  // subscribe to ClusterUp and ClusterDown before starting ClusterMonitor
  context.system.eventStream.subscribe(self, classOf[ClusterState])

  // state
  val coordinator = context.actorOf(settings.props, "coordinator")
  val clusterMonitor = context.actorOf(ClusterMonitor.props(settings.minNrMembers), "cluster-monitor")

  // these actors are created when cluster moves to UP
  var shardManager: ActorRef = null
  var shardBalancer: ActorRef = null
  var gossiper: ActorRef = null
  var clusterState: ClusterState = ClusterState.initialState

  log.info("initializing cluster mode")

  // we try to join immediately if seed nodes are specified
  override def preStart(): Unit = {
    val seedNodes = scala.collection.immutable.Seq(settings.seedNodes.map(AddressFromURIString(_)) :_*)
    seedNodes.headOption match {
      case None =>
        val nodes = scala.collection.immutable.Seq(selfAddress)
        Cluster(context.system).joinSeedNodes(nodes)
        log.info("starting seed with addresses {}", nodes.mkString(", "))
      case Some(address) if address.equals(selfAddress) =>
        val nodes = scala.collection.immutable.Seq(seedNodes.toSeq :_*)
        Cluster(context.system).joinSeedNodes(nodes)
        log.info("starting seed with addresses {}", nodes.mkString(", "))
      case Some(address) =>
        val nodes = seedNodes.filterNot(_.equals(selfAddress))
        Cluster(context.system).joinSeedNodes(nodes)
        log.info("joining cluster using seeds {}", nodes.mkString(", "))
    }
  }

  def receive = down

  // cluster is down, we just tell client to try again later
  def down: Receive = {

    // cluster monitor emits this message
    case state: ClusterUp =>
      clusterState =  state
      // create actors if they don't exist.  this should happen only once.
      if (gossiper == null) {
        gossiper = context.actorOf(DistributedPubSubMediator.props(None), "cluster-gossiper")
      }
      if (shardManager == null) {
        shardManager = context.actorOf(ShardManager.props(context.parent, propsCreator,
          entityReviver, selfAddress, settings.totalShards, gossiper), "entity-manager")
      }
      if (shardBalancer == null) {
        shardBalancer = context.actorOf(ClusterBalancer.props(settings, context.parent,
          shardManager.path.elements), "shard-balancer")
      }
      log.debug("cluster becomes UP")
      context.become(up)

    // cluster monitor emits this message
    case state: ClusterDown =>
      clusterState = state

    // return the current status for the specified node
    case op: GetNodeStatus =>
      val address = op.node.getOrElse(selfAddress)
      clusterState.getMember(address) match {
        case Some(member) =>
          val status = NodeStatus(selfUniqueAddress.address, selfUniqueAddress.uid, member.status, member.roles)
          sender() ! GetNodeStatusResult(op, status)
        case None =>
          sender() ! EntityServiceOperationFailed(op, ApiException(ResourceNotFound))
      }

    // return the current cluster status
    case op: GetClusterStatus =>
      val nodes = clusterState.members.map {
        case m => NodeStatus(m.uniqueAddress.address, m.uniqueAddress.uid, m.status, m.roles)
      }.toVector
      val leader = clusterState.getLeader
      val unreachable = clusterState.getUnreachable.map(_.address)
      val status = ClusterStatus(nodes, leader, unreachable)
      sender() ! GetClusterStatusResult(op, status)

    // return the current shard map status
    case op: GetShardMapStatus =>
      if (shardManager == null)
        sender() ! EntityServiceOperationFailed(op, ApiException(RetryLater))
      else shardManager.forward(op)

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
    case state: ClusterUp =>
      clusterState = state

    // cluster monitor emits this message
    case state: ClusterDown =>
      clusterState = state
      log.debug("cluster becomes DOWN")
      context.become(down)

    // return the current status for the specified node
    case op: GetNodeStatus =>
      val address = op.node.getOrElse(selfAddress)
      clusterState.getMember(address) match {
        case Some(member) =>
          val status = NodeStatus(selfUniqueAddress.address, selfUniqueAddress.uid, member.status, member.roles)
          sender() ! GetNodeStatusResult(op, status)
        case None =>
          sender() ! EntityServiceOperationFailed(op, ApiException(ResourceNotFound))
      }

    // return the current cluster status
    case op: GetClusterStatus =>
      val nodes = clusterState.members.map {
        case m => NodeStatus(m.uniqueAddress.address, m.uniqueAddress.uid, m.status, m.roles)
      }.toVector
      val leader = clusterState.getLeader
      val unreachable = clusterState.getUnreachable.map(_.address)
      val status = ClusterStatus(nodes, leader, unreachable)
      sender() ! GetClusterStatusResult(op, status)

    // return the current shard map status
    case op: GetShardMapStatus =>
      shardManager forward op

    // forward any messages for the coordinator
    case op: EntityServiceOperation =>
      coordinator forward op

    // send envelopes to the shard manager
    case envelope: EntityEnvelope =>
      shardManager ! envelope
  }

}
