package io.mandelbrot.core.cluster

import akka.actor._
import akka.cluster.{Member, Cluster}
import akka.cluster.ClusterEvent._
import scala.concurrent.duration._

/**
 *
 */
class ShardManager(entityManager: ActorRef, minNrMembers: Int, initialShards: Int) extends LoggingFSM[ShardManagerFSMState,ShardManagerFSMData] {
  import ShardManager._
  import context.dispatcher

  // config
  val rebalanceTimeout = 30.seconds
  val rebalanceInterval = 60.seconds
  val retryInterval = 3.seconds

  // state
  var rebalancer: Option[ActorRef] = None
  var nextRebalance: Option[Cancellable] = None
  var lsn = 1

  override def preStart(): Unit = {
    Cluster(context.system).subscribe(self, InitialStateAsEvents, classOf[ClusterDomainEvent])
  }

  override def postStop(): Unit = {
    Cluster(context.system).unsubscribe(self)
  }

  startWith(Incubating, Incubating(None, Set(Cluster(context.system).selfAddress)))

  when(Incubating) {

    /*
     * cluster membership events
     */
    case Event(MemberUp(member), state: Incubating) =>
      log.debug("member {} is UP", member)
      val members = state.members + member.address
      state.leader match {
        case Some(address) if !state.members.contains(address) =>
          stay() using Incubating(state.leader, members)
        case Some(address) =>
          if (members.size >= minNrMembers) {
            if (address == Cluster(context.system).selfAddress)
              goto(Leader) using Leader(members, None)
            else
              goto(Follower) using Follower(address, members, None)
          } else stay() using Incubating(state.leader, members)
        case None =>
          stay() using Incubating(state.leader, members)
      }

    case Event(leaderChanged: LeaderChanged, state: Incubating) =>
      leaderChanged.leader match {
        case Some(address) if state.members.contains(address) =>
          log.debug("{} is now the leader", address)
          if (state.members.size >= minNrMembers) {
            if (address == Cluster(context.system).selfAddress)
              goto(Leader) using Leader(state.members, None)
            else
              goto(Follower) using Follower(address, state.members, None)
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

    case Event(op: ApplyRebalancing, _) =>
      stay()  // drop message

    case Event(op: CommitRebalancing, _) =>
      stay()  // drop message
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
      nextRebalance = Some(context.system.scheduler.scheduleOnce(2.seconds, self, PerformRebalance(lsn)))

    case _ -> Incubating =>
      lsn = lsn + 1
      nextRebalance.foreach(_.cancel())
      nextRebalance = None
      context.system.eventStream.publish(ShardClusterDown)
      log.debug("shard cluster is DOWN, we incubate")
  }

  when(Leader) {

    /*
     * rebalance leader events
     */
    case Event(PerformRebalance(op), state: Leader) if op != lsn =>
      log.debug("ignoring rebalance request, lsn does not match")
      stay()

    case Event(PerformRebalance(op), Leader(members, inflight)) if inflight.isEmpty =>
      lsn = lsn + 1
      val proposed = Vector(InitializeRing(members.toVector, initialShards))
      context.actorOf(ShardRebalancer.props(lsn, proposed, Vector.empty, members.toVector, members, rebalanceTimeout))
      log.debug("starting initial shard balancing with lsn {}", lsn)
      log.debug("current members {}", members.mkString(", "))
      stay()

    case Event(RebalancingSucceeds(op), state: Leader) =>
      if (op == lsn) {
        log.debug("rebalancing {} succeeds, next rebalance in {}", op, rebalanceInterval)
        nextRebalance = Some(context.system.scheduler.scheduleOnce(rebalanceInterval, self, PerformRebalance(lsn)))
      }
      stay()

    case Event(RebalancingFails(op), state: Leader) =>
      if (op == lsn) {
        log.debug("rebalancing {} fails, retrying in {}", op, retryInterval)
        nextRebalance = Some(context.system.scheduler.scheduleOnce(retryInterval, self, PerformRebalance(lsn)))
      }
      stay()

    /*
     * rebalance 3PC events
     */
    case Event(op: SolicitRebalancing, state: Leader) =>
      val proposal = RebalanceProposal(op.lsn, sender(), op.proposed)
      stay() replying SolicitRebalancingResult(op.lsn, accepted = true) using state.copy(proposal = Some(proposal))

    case Event(ApplyRebalancing(op), state: Leader) =>
      state.proposal match {
        case Some(proposal) =>
          log.debug("applying mutations {} with lsn {} from {}", proposal.mutations, proposal.lsn, proposal.proposer)
          entityManager ! proposal
        case None =>
          log.debug("can't apply rebalancing with lsn {}, there is no proposal", op)
      }
      stay()

    case Event(AppliedProposal(proposal), state: Leader) =>
      proposal.proposer ! ApplyRebalancingResult(proposal.lsn)
      stay()

    case Event(CommitRebalancing(op), state: Leader) =>
      log.debug("all members have applied mutations with lsn {}", op)
      context.system.eventStream.publish(ShardClusterRebalances)
      stay() replying CommitRebalancingResult(op) using state.copy(proposal = None)

    /*
     * cluster membership events
     */
    case Event(MemberUp(member), state: Leader) =>
      log.debug("member {} is UP", member)
      stay() using state.copy(members = state.members + member.address)

    case Event(LeaderChanged(Some(address)), Leader(members, _)) =>
      goto(Follower) using Follower(address, members, None)

    case Event(MemberRemoved(member, status), Leader(members, _)) =>
      log.debug("member {} was removed", member)
      stay() using Leader(members - member.address, None)

    case Event(_: ClusterDomainEvent, _) =>
      stay()
  }

  onTransition {

    case Leader -> Follower =>
      lsn = lsn + 1
      nextRebalance.foreach(_.cancel())
      nextRebalance = None
      log.debug("we move from leader to follower")

    case Follower -> Leader =>
      lsn = lsn + 1
      log.debug("we move from leader to follower")
  }

  when(Follower) {

    /*
     * rebalance 3PC events
     */
    case Event(op: SolicitRebalancing, state: Follower) =>
      val proposal = RebalanceProposal(op.lsn, sender(), op.proposed)
      stay() replying SolicitRebalancingResult(op.lsn, accepted = true) using state.copy(proposal = Some(proposal))

    case Event(ApplyRebalancing(op), state: Follower) =>
      state.proposal match {
        case Some(proposal) =>
          log.debug("applying mutations {} with lsn {} from {}", proposal.mutations, proposal.lsn, proposal.proposer)
          entityManager ! proposal
        case None =>
          log.debug("can't apply rebalancing with lsn {}, there is no proposal", op)
      }
      stay()

    case Event(AppliedProposal(proposal), state: Follower) =>
      proposal.proposer ! ApplyRebalancingResult(proposal.lsn)
      stay()

    case Event(CommitRebalancing(op), state: Follower) =>
      log.debug("all members have applied mutations with lsn {}", op)
      context.system.eventStream.publish(ShardClusterRebalances)
      stay() replying CommitRebalancingResult(op) using state.copy(proposal = None)

    /*
     * cluster membership events
     */
    case Event(LeaderChanged(Some(address)), state: Follower) =>
      goto(Leader) using Leader(state.members, state.proposal)

    case Event(MemberUp(member), state: Follower) =>
      log.debug("member {} is UP", member)
      stay() using state.copy(members = state.members + member.address)

    case Event(MemberRemoved(member, status), state: Follower) =>
      log.debug("member {} was removed", member)
      stay() using state.copy(members = state.members - member.address)

    case Event(_: ClusterDomainEvent, _) =>
      stay()
  }

  initialize()
}

object ShardManager {
  def props(entityManager: ActorRef, minNrMembers: Int, initialShards: Int) = {
    Props(classOf[ShardManager], entityManager, minNrMembers, initialShards)
  }

  case class PerformRebalance(lsn: Int)
}

sealed trait ShardManagerFSMState
case object Incubating extends ShardManagerFSMState
case object Leader extends ShardManagerFSMState
case object Follower extends ShardManagerFSMState

sealed trait ShardManagerFSMData
case class Incubating(leader: Option[Address], members: Set[Address]) extends ShardManagerFSMData
case class Leader(members: Set[Address], proposal: Option[RebalanceProposal]) extends ShardManagerFSMData
case class Follower(leader: Address, members: Set[Address], proposal: Option[RebalanceProposal]) extends ShardManagerFSMData

sealed trait ShardManagerEvent
case object ShardClusterUp extends ShardManagerEvent
case object ShardClusterRebalances extends ShardManagerEvent
case object ShardClusterDown extends ShardManagerEvent
