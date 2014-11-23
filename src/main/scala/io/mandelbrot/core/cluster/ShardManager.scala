package io.mandelbrot.core.cluster

import akka.actor._
import akka.cluster.{MemberStatus, Cluster}
import akka.cluster.ClusterEvent._
import scala.concurrent.duration._

/**
 *
 */
class ShardManager extends LoggingFSM[ShardManagerFSMState,ShardManagerFSMData] {
  import ShardManager._
  import context.dispatcher

  // config
  val initialDelay = 3.seconds
  val rebalanceTimeout = 30.seconds
  val rebalanceInterval = 60.seconds
  val retryInterval = 3.seconds
  val selfUniqueAddress = Cluster(context.system).selfUniqueAddress
  val selfAddress = Cluster(context.system).selfAddress

  // state
  var hatched = false
  var rebalancer: Option[ActorRef] = None
  var nextRebalance: Option[Cancellable] = None
  var lsn = 1


  startWith(Incubating, Incubating(None, Set(selfAddress)))

  when(Incubating) {

    /*
     * cluster membership events
     */
    case Event(ClusterUp(leader, cluster), state: Incubating) =>
      val members = cluster.members.filter(_.status.equals(MemberStatus.up)).map(_.address).toSet
      if (leader == selfAddress)
        goto(Leader) using Leader(members, None)
      else
        goto(Follower) using Follower(leader, members, None)

    case Event(ClusterDown(cluster), state: Incubating) =>
      val members = cluster.members.filter(_.status.equals(MemberStatus.up)).map(_.address).toSet
      stay() using Incubating(cluster.leader, members)

    /*
     * rebalance 3PC events
     */
    case Event(SolicitRebalancing(op, _), _) =>
      log.debug("refused rebalance solicitation, we are incubating")
      stay() replying SolicitRebalancingResult(op, accepted = false)

    case Event(op: ApplyRebalancing, _) =>
      log.debug("refused to apply rebalance, we are incubating")
      stay()  // drop message

    case Event(op: CommitRebalancing, _) =>
      log.debug("refused to commit rebalance, we are incubating")
      stay()  // drop message
  }

  onTransition {
    case Incubating -> Follower =>
      lsn = lsn + 1
      log.debug("shard cluster is UP, we are a follower")
      log.debug("follower state is {}", nextStateData)

    case Incubating -> Leader =>
      lsn = lsn + 1
      log.debug("shard cluster is UP, we are the leader")
      log.debug("leader state is {}", nextStateData)
      nextRebalance = Some(context.system.scheduler.scheduleOnce(initialDelay, self, PerformRebalance(lsn)))

    case _ -> Incubating =>
      lsn = lsn + 1
      nextRebalance.foreach(_.cancel())
      nextRebalance = None
      log.debug("shard cluster is DOWN, we incubate")
  }

  when(Leader) {

    /*
     * rebalance leader events
     */
    case Event(op: PerformRebalance, state: Leader) =>
      if (op.lsn == lsn) {
        state.proposal match {
          case Some(proposal) =>
          case None =>
            lsn = lsn + 1
            val members = state.members.toVector
            val mutations = Vector.empty[ShardRingMutation]
            val name = "rebalancer-%d-%d".format(lsn, System.currentTimeMillis())
            context.actorOf(ShardRebalancer.props(lsn, mutations, Vector.empty, members, state.members, rebalanceTimeout), name)
            log.debug("began shard balancing round with lsn {}", lsn)
            log.debug("current members {}", members.mkString(", "))
        }
      } else log.debug("ignoring rebalance request with lsn {}, current lsn is {}", op.lsn, lsn)
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
      log.debug("accepted rebalance solicitation with lsn {}", op.lsn)
      val proposal = RebalanceProposal(op.lsn, sender(), op.proposed)
      stay() replying SolicitRebalancingResult(op.lsn, accepted = true) using state.copy(proposal = Some(proposal))

    case Event(ApplyRebalancing(op), state: Leader) =>
      state.proposal match {
        case Some(proposal) =>
          log.debug("applying mutations {} with lsn {} from {}", proposal.mutations, proposal.lsn, proposal.proposer)
          context.parent ! proposal
        case None =>
          log.debug("can't apply rebalancing with lsn {}, there is no proposal", op)
      }
      stay()

    case Event(AppliedProposal(proposal), state: Leader) =>
      log.debug("locally applied rebalance proposal with lsn {}", proposal.lsn)
      proposal.proposer ! ApplyRebalancingResult(proposal.lsn)
      stay()

    case Event(CommitRebalancing(op), state: Leader) =>
      log.debug("all members have applied proposal with lsn {}", op)
      context.parent ! ShardsRebalanced
      stay() replying CommitRebalancingResult(op) using state.copy(proposal = None)

    /*
     * cluster membership events
     */
    case Event(ClusterUp(leader, cluster), state: Leader) =>
      val members = cluster.members.filter(_.status.equals(MemberStatus.up)).map(_.address).toSet
      if (leader == selfAddress)
        stay() using state.copy(members = members)
      else
        goto(Follower) using Follower(leader, members, state.proposal)

    case Event(ClusterDown(cluster), Leader(members, _)) =>
      val members = cluster.members.filter(_.status.equals(MemberStatus.up)).map(_.address).toSet
      goto(Incubating) using Incubating(cluster.leader, members)
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
      log.debug("accepted rebalance solicitation with lsn {}", op.lsn)
      val proposal = RebalanceProposal(op.lsn, sender(), op.proposed)
      stay() replying SolicitRebalancingResult(op.lsn, accepted = true) using state.copy(proposal = Some(proposal))

    case Event(ApplyRebalancing(op), state: Follower) =>
      state.proposal match {
        case Some(proposal) =>
          log.debug("applying mutations {} with lsn {} from {}", proposal.mutations, proposal.lsn, proposal.proposer)
          context.parent ! proposal
        case None =>
          log.debug("can't apply rebalancing with lsn {}, there is no proposal", op)
      }
      stay()

    case Event(AppliedProposal(proposal), state: Follower) =>
      log.debug("locally applied rebalance proposal with lsn {}", proposal.lsn)
      proposal.proposer ! ApplyRebalancingResult(proposal.lsn)
      stay()

    case Event(CommitRebalancing(op), state: Follower) =>
      log.debug("all members have applied proposal with lsn {}", op)
      context.parent ! ShardsRebalanced
      stay() replying CommitRebalancingResult(op) using state.copy(proposal = None)

    /*
     * cluster membership events
     */
    case Event(ClusterUp(leader, cluster), state: Follower) =>
      val members = cluster.members.filter(_.status.equals(MemberStatus.up)).map(_.address).toSet
      if (leader != selfAddress)
        stay() using state.copy(members = members)
      else
        goto(Leader) using Leader(members, state.proposal)

    case Event(ClusterDown(cluster), Leader(members, _)) =>
      val members = cluster.members.filter(_.status.equals(MemberStatus.up)).map(_.address).toSet
      goto(Incubating) using Incubating(cluster.leader, members)
  }

  initialize()
}

object ShardManager {
  def props() = Props(classOf[ShardManager])

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

case object ShardsRebalanced
