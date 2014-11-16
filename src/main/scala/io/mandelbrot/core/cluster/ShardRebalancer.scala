package io.mandelbrot.core.cluster

import java.util.UUID

import akka.actor._
import scala.concurrent.duration.FiniteDuration

/**
 *
 */
class ShardRebalancer(lsn: Int, proposed: ShardRing, shrinks: Vector[Address], grows: Vector[Address], peers: Set[Address], timeout: FiniteDuration)
extends LoggingFSM[ShardRebalancerFSMState,ShardRebalancerFSMData] {
  import ShardRebalancer._
  import context.dispatcher

  // the set of addresses whose set of shards does not grow or shrink
  val updates: Vector[Address] = ((peers.toSet[Address] -- shrinks) -- grows).toVector

  // a cached mapping of Addresses to ActorRefs
  var refs = Map.empty[Address,ActorRef]

  startWith(Soliciting, Soliciting(Set.empty))
  context.system.scheduler.scheduleOnce(timeout, self, RebalancingTimeout)

  peers.foreach { address =>
    val selection = context.system.actorSelection(RootActorPath(address) / context.parent.path.elements)
    selection ! SolicitRebalancing(lsn, proposed)
  }
  log.debug("beginning rebalancing with id {}", lsn)

  when(Soliciting) {

    case Event(result: SolicitRebalancingResult, _) if result.lsn != lsn =>
      stay()

    case Event(result: SolicitRebalancingResult, data: Soliciting) =>
      if (result.accepted) {
        log.debug("peer {} accepts solicitation for rebalance id {}", sender(), lsn)
        refs = refs + (sender().path.address -> sender())
        val replies = data.replies + sender().path.address
        if (replies.size == peers.size) {
          if (shrinks.nonEmpty)
            goto(Shrinking) using Shrinking(shrinks ++ updates)
          else
            goto(Growing) using Growing(updates ++ grows)
        } else stay() using Soliciting(replies)
      } else {
        log.debug("peer {} refuses solicitation for rebalance id {}", sender(), lsn)
        context.parent ! RebalancingFails(lsn)
        stop()
      }

    case Event(RebalancingTimeout, _) =>
      log.debug("rebalancing timed out")
      context.parent ! RebalancingFails(lsn)
      stop()
  }

  onTransition {
    case Soliciting -> Shrinking =>
      refs(shrinks.head) ! ApplyRebalancing(lsn)
  }

  when(Shrinking) {

    case Event(result: ApplyRebalancingResult, _) if result.lsn != lsn =>
      stay()

    case Event(result: ApplyRebalancingResult, data: Shrinking) =>
      log.debug("shrinking peer {} for rebalance id {}", sender(), lsn)
      if (data.left.length == 1) {
        if (grows.nonEmpty)
          goto(Growing) using Growing(grows)
        else
          goto(Committing) using Committing(Set.empty)
      } else {
        val left = data.left.tail
        refs(left.head) ! ApplyRebalancing(lsn)
        stay() using Shrinking(left)
      }

    case Event(RebalancingTimeout, _) =>
      log.debug("rebalancing timed out")
      context.parent ! RebalancingFails(lsn)
      stop()
  }

  onTransition {
    case _ -> Growing =>
      refs(grows.head) ! ApplyRebalancing(lsn)
  }

  when(Growing) {

    case Event(result: ApplyRebalancingResult, _) if result.lsn != lsn =>
      stay()

    case Event(result: ApplyRebalancingResult, data: Growing) =>
      log.debug("growing peer {} for rebalance id {}", sender(), lsn)
      if (data.left.length == 1) {
        goto(Committing) using Committing(Set.empty)
      } else {
        val left = data.left.tail
        refs(left.head) ! ApplyRebalancing(lsn)
        stay() using Growing(left)
      }

    case Event(RebalancingTimeout, _) =>
      log.debug("rebalancing timed out")
      context.parent ! RebalancingFails(lsn)
      stop()
  }

  onTransition {
    case _ -> Committing =>
      peers.foreach(refs(_) ! CommitRebalancing(lsn))
  }

  when(Committing) {

    case Event(result: CommitRebalancingResult, _) if result.lsn != lsn =>
      stay()

    case Event(result: CommitRebalancingResult, data: Committing) =>
      log.debug("peer {} commits rebalance id {}", sender(), lsn)
      val replies = data.replies + sender().path.address
      if (replies.size == peers.size) {
        log.debug("rebalancing complete for id {}", lsn)
        context.parent ! RebalancingSucceeds(lsn)
        stop()
      } else stay() using Committing(replies)

    case Event(RebalancingTimeout, _) =>
      log.debug("rebalancing timed out")
      context.parent ! RebalancingFails(lsn)
      stop()
  }

  initialize()
}

object ShardRebalancer {

  def props(lsn: Int,
            proposed: ShardRing,
            shrinks: Vector[Address],
            grows: Vector[Address],
            peers: Set[Address],
            timeout: FiniteDuration) = {
    Props(classOf[ShardRebalancer], lsn, proposed, shrinks, grows, peers, timeout)
  }

  case object RebalancingTimeout
}

sealed trait ShardRebalancerFSMState
case object Soliciting extends ShardRebalancerFSMState
case object Shrinking extends ShardRebalancerFSMState
case object Growing extends ShardRebalancerFSMState
case object Committing extends ShardRebalancerFSMState

sealed trait ShardRebalancerFSMData
case class Soliciting(replies: Set[Address]) extends ShardRebalancerFSMData
case class Shrinking(left: Vector[Address]) extends ShardRebalancerFSMData
case class Growing(left: Vector[Address]) extends ShardRebalancerFSMData
case class Committing(replies: Set[Address]) extends ShardRebalancerFSMData

case class SolicitRebalancing(lsn: Int, proposed: ShardRing)
case class SolicitRebalancingResult(lsn: Int, accepted: Boolean)

case class ApplyRebalancing(lsn: Int)
case class ApplyRebalancingResult(lsn: Int)

case class CommitRebalancing(lsn: Int)
case class CommitRebalancingResult(lsn: Int)

case class RebalancingSucceeds(lsn: Int)
case class RebalancingFails(lsn: Int)