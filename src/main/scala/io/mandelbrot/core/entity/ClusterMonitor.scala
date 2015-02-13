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

import akka.cluster.{MemberStatus, Cluster}
import akka.cluster.ClusterEvent._
import akka.actor._

/**
 * The ClusterMonitor listens for akka cluster domain events and decides when
 * the cluster is maintaining a quorum.
 */
class ClusterMonitor(minNrMembers: Int) extends Actor with ActorLogging {

  // state
  var running = false

  override def preStart(): Unit = {
    Cluster(context.system).subscribe(self, classOf[ClusterDomainEvent])
  }

  override def postStop(): Unit = {
    Cluster(context.system).unsubscribe(self)
  }

  def receive = {

    case MemberUp(member) =>
      log.debug("cluster member {} is now UP", member)
      sendCurrentState(currentState)

    case MemberExited(member) =>
      log.debug("cluster member {} exits", member)
      sendCurrentState(currentState)

    case MemberRemoved(member, previous) =>
      log.debug("cluster member {} has been removed", member)
      sendCurrentState(currentState)

    case LeaderChanged(leader) =>
      log.debug("cluster leader changes to {}", leader)
      sendCurrentState(currentState)

    case ReachableMember(member) =>
      log.debug("cluster member {} becomes reachable", member)
      sendCurrentState(currentState)

    case UnreachableMember(member) =>
      log.debug("cluster member {} becomes unreachable", member)
      sendCurrentState(currentState)

    case event: ClusterDomainEvent =>
      //log.debug("ignoring cluster domain event {}", event)
  }

  def currentState: ClusterMonitorEvent = {
    val state = Cluster(context.system).state
    val membersUp = state.members.foldLeft(0) {
      case (numUp, member) => if (member.status.equals(MemberStatus.up)) numUp + 1 else numUp
    }
    if (membersUp < minNrMembers || state.leader.isEmpty)
      ClusterDown(state)
    else
      ClusterUp(state.leader.get, state)
  }

  def sendCurrentState(currentState: ClusterMonitorEvent): Unit = {
    context.system.eventStream.publish(currentState)
  }
}

object ClusterMonitor {
  def props(minNrMembers: Int) = Props(classOf[ClusterMonitor], minNrMembers)
}

sealed trait ClusterMonitorEvent { val state: CurrentClusterState }
case class ClusterUp(leader: Address, state: CurrentClusterState) extends ClusterMonitorEvent
case class ClusterDown(state: CurrentClusterState) extends ClusterMonitorEvent
