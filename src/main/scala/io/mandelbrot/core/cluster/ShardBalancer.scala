package io.mandelbrot.core.cluster

import akka.actor._
import akka.cluster.{MemberStatus, Cluster}
import scala.concurrent.duration._

/**
 *
 */
class ShardBalancer extends LoggingFSM[ShardManagerFSMState,ShardManagerFSMData] {
  import ShardBalancer._
  import context.dispatcher

  // config
  val initialDelay = 3.seconds
  val rebalanceTimeout = 30.seconds
  val rebalanceInterval = 95.seconds
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
    case Event(ClusterUp(leader, cluster), state: Incubating) =>
      val members = cluster.members.filter(_.status.equals(MemberStatus.up)).map(_.address).toSet
      if (leader == selfAddress)
        goto(Leader) using Leader(members, None)
      else
        goto(Follower) using Follower(leader, members, None)

    case Event(ClusterDown(cluster), state: Incubating) =>
      val members = cluster.members.filter(_.status.equals(MemberStatus.up)).map(_.address).toSet
      stay() using Incubating(cluster.leader, members)
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
      hatched = false
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
            log.debug("ignoring rebalance request, proposal {} is in-flight", proposal)
          case None if !hatched =>
            // FIXME: implement shard migrations
          case None if hatched =>
            lsn = lsn + 1
            val members = state.members.toVector
            val mutations = Vector.empty[ShardRingMutation]
            val name = "rebalancer-%d-%d".format(lsn, System.currentTimeMillis())
            context.actorOf(ShardRebalancer.props(lsn, mutations, Vector.empty, members, state.members, rebalanceTimeout), name)
        }
      } else log.debug("ignoring rebalance request with lsn {}, current lsn is {}", op.lsn, lsn)
      stay()

    case Event(RebalanceResult(succeeded, op), state: Leader) =>
      if (op == lsn) {
        if (succeeded) {
          log.debug("rebalance {} succeeds, next rebalance in {}", op, rebalanceInterval)
          nextRebalance = Some(context.system.scheduler.scheduleOnce(rebalanceInterval, self, PerformRebalance(lsn)))
        } else {
          log.debug("rebalance {} fails, retrying in {}", op, retryInterval)
          nextRebalance = Some(context.system.scheduler.scheduleOnce(retryInterval, self, PerformRebalance(lsn)))
        }
      }
      stay()

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

object ShardBalancer {
  def props() = Props(classOf[ShardBalancer])

  case class PerformRebalance(lsn: Int)
}

sealed trait ShardManagerFSMState
case object Incubating extends ShardManagerFSMState
case object Leader extends ShardManagerFSMState
case object Follower extends ShardManagerFSMState

sealed trait ShardManagerFSMData
case class Incubating(leader: Option[Address], members: Set[Address]) extends ShardManagerFSMData
case class Leader(members: Set[Address], proposal: Option[Any]) extends ShardManagerFSMData
case class Follower(leader: Address, members: Set[Address], proposal: Option[Any]) extends ShardManagerFSMData

case object ShardsRebalanced
