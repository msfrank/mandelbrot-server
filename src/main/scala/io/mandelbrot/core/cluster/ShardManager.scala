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
  import context.dispatcher

  // config
  val timeout = 30.seconds

  // state
  var current: Option[ShardRing] = None
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

    /*
     * cluster membership events
     */
    case Event(MemberUp(member), state: Incubating) =>
      log.debug("member {} is UP", member)
      val members = state.members + (member.address -> member)
      state.leader match {
        case Some(address) if !state.members.contains(address) =>
          stay() using Incubating(state.leader, members)
        case Some(address) =>
          if (members.size >= minNrMembers) {
            if (address == Cluster(context.system).selfAddress)
              goto(Leader) using Leader(state.members, None)
            else
              goto(Follower) using Follower(state.members(address), state.members, None)
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
              goto(Leader) using Leader(state.members, None)
            else
              goto(Follower) using Follower(state.members(address), state.members, None)
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

    /*
     * rebalance 3PC events
     */
    case Event(SolicitRebalancing(op, _), _) =>
      stay() replying SolicitRebalancingResult(op, accepted = false)
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
      context.system.scheduler.scheduleOnce(2.seconds, self, PerformRebalance(lsn))

    case _ -> Incubating =>
      lsn = lsn + 1
      context.system.eventStream.publish(ShardClusterDown)
      log.debug("shard cluster is DOWN, we incubate")
  }

  when(Leader) {

    /*
     * rebalance leader events
     */
    case Event(PerformRebalance(op), Leader(members, _)) if op != lsn =>
      log.debug("ignoring PerformRebalance, lsn does not match")
      stay()

    case Event(PerformRebalance(op), Leader(members, _)) if current.isEmpty =>
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

    /*
     * rebalance 3PC events
     */
    case Event(SolicitRebalancing(op, proposed), state: Leader) =>
      stay() replying SolicitRebalancingResult(op, accepted = true) using state.copy(proposal = Some(proposed))

    case Event(ApplyRebalancing(op), state: Leader) =>
      current = state.proposal
      stay() replying ApplyRebalancingResult(op)

    case Event(CommitRebalancing(op), state: Leader) =>
      stay() replying CommitRebalancingResult(op)

    /*
     * cluster membership events
     */
    case Event(LeaderChanged(Some(address)), Leader(members, _)) =>
      goto(Follower) using Follower(members(address), members, None)

    case Event(MemberUp(member), Leader(members, _)) =>
      stay() using Leader(members + (member.address -> member), None)

    case Event(MemberRemoved(member, status), Leader(members, _)) =>
      stay() using Leader(members - member.address, None)

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

    /*
     * rebalance 3PC events
     */
    case Event(SolicitRebalancing(op, proposed), state: Follower) =>
      stay() replying SolicitRebalancingResult(op, accepted = true) using state.copy(proposal = Some(proposed))

    case Event(ApplyRebalancing(op), state: Follower) =>
      current = state.proposal
      stay() replying ApplyRebalancingResult(op)

    case Event(CommitRebalancing(op), state: Follower) =>
      stay() replying CommitRebalancingResult(op)

    /*
     * cluster membership events
     */
    case Event(LeaderChanged(Some(address)), _) =>
      stay()

    case Event(MemberUp(member), Follower(leader, members, _)) =>
      stay() using Follower(leader, members + (member.address -> member), None)

    case Event(MemberRemoved(member, status), Follower(leader, members, _)) =>
      stay() using Follower(leader, members - member.address, None)

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

  case class PerformRebalance(lsn: Int)
}

sealed trait ShardManagerFSMState
case object Incubating extends ShardManagerFSMState
case object Leader extends ShardManagerFSMState
case object Follower extends ShardManagerFSMState

sealed trait ShardManagerFSMData
case class Incubating(leader: Option[Address], members: Map[Address,Member]) extends ShardManagerFSMData
case class Leader(members: Map[Address,Member], proposal: Option[ShardRing]) extends ShardManagerFSMData
case class Follower(leader: Member, members: Map[Address,Member], proposal: Option[ShardRing]) extends ShardManagerFSMData

sealed trait ShardManagerEvent
case object ShardClusterUp extends ShardManagerEvent
case object ShardClusterRebalances extends ShardManagerEvent
case object ShardClusterDown extends ShardManagerEvent

case class ShardEnvelope(address: String, message: Any)
case class DeliverMessage(address: String, message: Any)

