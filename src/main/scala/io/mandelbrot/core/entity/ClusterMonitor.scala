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
import akka.cluster.{UniqueAddress, Member, MemberStatus, Cluster}
import akka.cluster.ClusterEvent._
import scala.collection.SortedSet

/**
 * The ClusterMonitor listens for akka cluster domain events and decides when
 * the cluster is maintaining a quorum.
 */
class ClusterMonitor(minNrMembers: Int) extends Actor with ActorLogging {

  // state
  var members: SortedSet[Member] = SortedSet.empty
  var leader: Option[Address] = None
  var unreachable: Set[Member] = Set.empty

  override def preStart(): Unit = {
    Cluster(context.system).subscribe(self, initialStateMode = initialStateAsSnapshot, classOf[ClusterDomainEvent])
  }

  override def postStop(): Unit = {
    Cluster(context.system).unsubscribe(self)
    members = SortedSet.empty
    leader = None
    unreachable = Set.empty
  }

  def receive = {

    case state: CurrentClusterState =>
      members = state.members
      leader = state.leader
      unreachable = state.unreachable
      log.debug("received initial cluster state")
      sendCurrentState(currentState)

    case MemberUp(member) =>
      members = members + member
      log.debug("cluster member {} is now UP", member)
      sendCurrentState(currentState)

    case MemberExited(member) =>
      members = members + member
      log.debug("cluster member {} exits", member)
      sendCurrentState(currentState)

    case MemberRemoved(member, previous) =>
      members = members - member
      log.debug("cluster member {} has been removed", member)
      sendCurrentState(currentState)

    case LeaderChanged(newLeader) =>
      leader = newLeader
      log.debug("cluster leader changes to {}", leader)
      sendCurrentState(currentState)

    case ReachableMember(member) =>
      unreachable = unreachable - member
      log.debug("cluster member {} becomes reachable", member)
      sendCurrentState(currentState)

    case UnreachableMember(member) =>
      unreachable = unreachable + member
      log.debug("cluster member {} becomes unreachable", member)
      sendCurrentState(currentState)

    case event: ClusterMetricsChanged =>
      // ignore this message for now
  }

  def currentState: ClusterState = {
    val membersUp = members.foldLeft(0) {
      case (numUp, member) => if (member.status.equals(MemberStatus.up)) numUp + 1 else numUp
    }
    if (membersUp < minNrMembers || leader.isEmpty)
      ClusterDown(leader, members)
    else
      ClusterUp(leader.get, members, unreachable)
  }

  def sendCurrentState(currentState: ClusterState): Unit = {
    context.system.eventStream.publish(currentState)
  }
}

object ClusterMonitor {
  def props(minNrMembers: Int) = Props(classOf[ClusterMonitor], minNrMembers)
}

sealed trait ClusterState {
  val members: SortedSet[Member]
  def getMember(address: Address): Option[Member] = members.find(_.uniqueAddress.address.equals(address))
  def getMember(uniqueAddress: UniqueAddress): Option[Member] = members.find(_.uniqueAddress.equals(uniqueAddress))
  def getLeader: Option[Address]
  def getUnreachable: Set[Member]
}

object ClusterState {
  val initialState = ClusterDown(None, SortedSet.empty)
}
case class ClusterUp(leader: Address, members: SortedSet[Member], unreachable: Set[Member]) extends ClusterState {
  def getLeader = Some(leader)
  def getUnreachable = unreachable
}
case class ClusterDown(leader: Option[Address], members: SortedSet[Member]) extends ClusterState {
  def getLeader = leader
  def getUnreachable = Set.empty
}
