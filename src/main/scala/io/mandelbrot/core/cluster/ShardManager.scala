package io.mandelbrot.core.cluster

import akka.actor._
import akka.cluster.{Member, Cluster}
import akka.cluster.ClusterEvent._
import scala.concurrent.duration._

/**
 *
 */
class ShardManager(minNrMembers: Int, initialShards: Int) extends LoggingFSM[ShardManagerFSMState,ShardManagerFSMData] {
  import ShardManager._

  // config
  val timeout = 30.seconds

  // state
  var current: Option[ShardRing] = None
  var proposed: Option[(ActorRef,ShardRing)] = None
  var rebalancer: Option[ActorRef] = None
  var lsn = 1

  override def preStart(): Unit = {
    Cluster(context.system).subscribe(self, InitialStateAsEvents, classOf[ClusterDomainEvent])
  }

  override def postStop(): Unit = {
    Cluster(context.system).unsubscribe(self)
  }

  startWith(Incubating, Incubating(None, Map.empty))

  when(Incubating) {

    case Event(MemberUp(member), state: Incubating) =>
      log.debug("member {} is UP", member)
      val members = state.members + (member.address -> member)
      state.leader match {
        case Some(address) if !state.members.contains(address) =>
          stay() using Incubating(state.leader, members)
        case Some(address) =>
          if (members.size >= minNrMembers) {
            if (address == Cluster(context.system).selfAddress)
              goto(Leader) using Leader(state.members)
            else
              goto(Follower) using Follower(state.members(address), state.members)
          } else stay() using Incubating(state.leader, members)
        case None =>
          stay() using Incubating(state.leader, members)
      }

    case Event(leaderChanged: LeaderChanged, state: Incubating) =>
      leaderChanged.leader match {
        case Some(address) if state.members.contains(address) =>
          log.debug("{} is now the leader", state.members(address))
          if (state.members.size >= minNrMembers) {
            if (address == Cluster(context.system).selfAddress)
              goto(Leader) using Leader(state.members)
            else
              goto(Follower) using Follower(state.members(address), state.members)
          } else stay() using Incubating(leaderChanged.leader, state.members)
        case Some(address) =>
          log.warning("{} is now the leader but there is no known member with that address", address)
          stay() using Incubating(leaderChanged.leader, state.members)
        case None =>
          log.warning("leader is lost")
          stay() using Incubating(leaderChanged.leader, state.members)
      }

    case Event(_: ClusterDomainEvent, _) =>
      stay()
  }

  onTransition {
    case Incubating -> Follower =>
      lsn = lsn + 1
      context.system.eventStream.publish(ShardClusterUp)
      log.debug("shard cluster is UP, we are a follower")

    case Incubating -> Leader =>
      lsn = lsn + 1
      context.system.eventStream.publish(ShardClusterUp)
      log.debug("shard cluster is UP, we are the leader")
      self ! PerformRebalance

    case _ -> Incubating =>
      lsn = lsn + 1
      context.system.eventStream.publish(ShardClusterDown)
      log.debug("shard cluster is DOWN, we incubate")
  }

  when(Leader) {

    case Event(PerformRebalance, Leader(members)) if current.isEmpty =>
      lsn = lsn + 1
      val shardRing = ShardRing(members.keySet, initialShards)
      context.actorOf(ShardRebalancer.props(lsn, shardRing, Vector.empty, members.keys.toVector, members.keySet, timeout))
      log.debug("starting initial shard balancing with lsn {}", lsn)
      stay()

    case Event(RebalancingSucceeds(op), state: Leader) =>
      if (op == lsn) {
        context.system.eventStream.publish(ShardClusterRebalances)
        log.debug("rebalancing {} succeeds", op)
      }
      stay()

    case Event(RebalancingFails(op), state: Leader) =>
      if (op == lsn)
        log.debug("rebalancing {} fails", op)
      stay()

    case Event(LeaderChanged(Some(address)), Leader(members)) =>
      goto(Follower) using Follower(members(address), members)

    case Event(MemberUp(member), Leader(members)) =>
      stay() using Leader(members + (member.address -> member))

    case Event(MemberRemoved(member, status), Leader(members)) =>
      stay() using Leader(members - member.address)

    case Event(_: ClusterDomainEvent, _) =>
      stay()

    case Event(envelope: ShardEnvelope, _) =>
      stay()
  }

  onTransition {
    case Leader -> Follower =>
      lsn = lsn + 1
      log.debug("we move from leader to follower")
  }

  when(Follower) {

    case Event(LeaderChanged(Some(address)), _) =>
      stay()

    case Event(MemberUp(member), Follower(leader, members)) =>
      stay() using Follower(leader, members + (member.address -> member))

    case Event(MemberRemoved(member, status), Follower(leader, members)) =>
      stay() using Follower(leader, members - member.address)

    case Event(_: ClusterDomainEvent, _) =>
      stay()

    case Event(envelope: ShardEnvelope, _) =>
      stay()
  }

  onTransition {
    case Follower -> Leader =>
      lsn = lsn + 1
      log.debug("we move from leader to follower")
  }

  initialize()
}

object ShardManager {
  def props(minNrMembers: Int, initialShards: Int) = Props(classOf[ShardManager], minNrMembers, initialShards)

  case object PerformRebalance
}

sealed trait ShardManagerFSMState
case object Incubating extends ShardManagerFSMState
case object Leader extends ShardManagerFSMState
case object Follower extends ShardManagerFSMState

sealed trait ShardManagerFSMData
case class Incubating(leader: Option[Address], members: Map[Address,Member]) extends ShardManagerFSMData
case class Leader(members: Map[Address,Member]) extends ShardManagerFSMData
case class Follower(leader: Member, members: Map[Address,Member]) extends ShardManagerFSMData

sealed trait ShardManagerEvent
case object ShardClusterUp extends ShardManagerEvent
case object ShardClusterRebalances extends ShardManagerEvent
case object ShardClusterDown extends ShardManagerEvent

case class ShardEnvelope(address: String, message: Any)
case class DeliverMessage(address: String, message: Any)

