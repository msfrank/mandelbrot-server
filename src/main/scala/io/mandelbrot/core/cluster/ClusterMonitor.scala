package io.mandelbrot.core.cluster

import akka.cluster.{MemberStatus, Cluster}
import akka.cluster.ClusterEvent._
import akka.actor._

/**
 * 
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
      log.debug("ignoring cluster domain event {}", event)
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
    context.parent ! currentState
    context.system.eventStream.publish(currentState)
  }
}

object ClusterMonitor {
  def props(minNrMembers: Int) = Props(classOf[ClusterMonitor], minNrMembers)
}

sealed trait ClusterMonitorEvent
case class ClusterUp(leader: Address, state: CurrentClusterState) extends ClusterMonitorEvent
case class ClusterDown(state: CurrentClusterState) extends ClusterMonitorEvent
case object ClusterUnknown extends ClusterMonitorEvent
