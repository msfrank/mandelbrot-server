package io.mandelbrot.core.cluster

import akka.actor._
import akka.cluster.{Member, Cluster}
import akka.cluster.ClusterEvent._
import java.util

/**
 *
 */
class ShardManager extends LoggingFSM[ShardManagerFSMState,ShardManagerFSMData] {

  // config
  val numShards = 1000
  val minNrMembers = 1

  // state
  val regions = new util.HashMap[ActorRef,Vector[Long]](16)
  val shards = new util.HashMap[Long,ActorRef](numShards)

  override def preStart(): Unit = Cluster(context.system).subscribe(self, InitialStateAsEvents, classOf[ClusterDomainEvent])

  override def postStop(): Unit = Cluster(context.system).unsubscribe(self)

  startWith(Incubating, Incubating(None, Map.empty))

  when(Incubating) {

    case Event(leaderChanged: LeaderChanged, state: Incubating) =>
      leaderChanged.leader match {
        case Some(address) if state.members.contains(address) =>
          log.debug("{} is now the leader", state.members(address))
          if (state.members.size >= minNrMembers) {
            if (address == Cluster(context.system).selfAddress)
              goto(Leader) using Leader(state.members.values.toSet)
            else
              goto(Follower) using Follower(state.members(address))
          } else stay() using Incubating(leaderChanged.leader, state.members)
        case Some(address) =>
          log.warning("{} is now the leader but there is no known member with that address", address)
          stay() using Incubating(leaderChanged.leader, state.members)
        case None =>
          log.warning("leader is lost")
          stay() using Incubating(leaderChanged.leader, state.members)
      }

    case Event(MemberUp(member), state: Incubating) =>
      log.debug("member {} is UP", member)
      val members = state.members + (member.address -> member)
      state.leader match {
        case Some(address) if !state.members.contains(address) =>
          stay() using Incubating(state.leader, members)
        case Some(address) =>
          if (members.size >= minNrMembers) {
            if (address == Cluster(context.system).selfAddress)
              goto(Leader) using Leader(state.members.values.toSet)
            else
              goto(Follower) using Follower(state.members(address))
          } else stay() using Incubating(state.leader, members)
        case None =>
          stay() using Incubating(state.leader, members)
      }

    case Event(_: ClusterDomainEvent, _) =>
      stay()
  }

  onTransition {
    case Incubating -> _ =>
      context.system.eventStream.publish(ShardClusterUp)
      log.debug("shard cluster is UP")
    case _ -> Incubating =>
      context.system.eventStream.publish(ShardClusterDown)
      log.debug("shard cluster is DOWN")
  }

  when(Leader) {

    case Event(LeaderChanged(Some(address)), _) =>
      stay()

    case Event(MemberUp(member), _) =>
      stay()

    case Event(MemberExited(member), _) =>
      stay()

    case Event(MemberRemoved(member, status), _) =>
      stay()

    case Event(ReachableMember(member), _) =>
      stay()

    case Event(UnreachableMember(member), _) =>
      stay()

    case Event(_: ClusterDomainEvent, _) =>
      stay()
  }

  when(Follower) {

    case Event(LeaderChanged(Some(address)), _) =>
      stay()

    case Event(MemberUp(member), _) =>
      stay()

    case Event(MemberExited(member), _) =>
      stay()

    case Event(MemberRemoved(member, status), _) =>
      stay()

    case Event(ReachableMember(member), _) =>
      stay()

    case Event(UnreachableMember(member), _) =>
      stay()

    case Event(_: ClusterDomainEvent, _) =>
      stay()
  }

  initialize()
}

object ShardManager {
  def props() = Props(classOf[ShardManager])
}

sealed trait ShardManagerFSMState
case object Incubating extends ShardManagerFSMState
case object Leader extends ShardManagerFSMState
case object Follower extends ShardManagerFSMState

sealed trait ShardManagerFSMData
case class Incubating(leader: Option[Address], members: Map[Address,Member]) extends ShardManagerFSMData
case class Leader(members: Set[Member]) extends ShardManagerFSMData
case class Follower(leader: Member) extends ShardManagerFSMData

sealed trait ShardManagerEvent
case object ShardClusterUp extends ShardManagerEvent
case object ShardClusterDown extends ShardManagerEvent
