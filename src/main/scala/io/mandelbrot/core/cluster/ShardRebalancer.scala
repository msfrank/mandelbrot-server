package io.mandelbrot.core.cluster

import java.util.UUID

import akka.actor._
import scala.concurrent.duration.FiniteDuration

/**
 *
 */
class ShardRebalancer(id: String, current: ShardRing, proposed: ShardRing, shrinks: Vector[ActorRef], grows: Vector[ActorRef], peers: Set[ActorRef], timeout: FiniteDuration)
extends LoggingFSM[ShardRebalancerFSMState,ShardRebalancerFSMData] {
  import ShardRebalancer._
  import context.dispatcher

  val updates: Vector[ActorRef] = ((peers.toSet[ActorRef] -- shrinks) -- grows).toVector

  startWith(Soliciting, Soliciting(Set.empty))
  context.system.scheduler.scheduleOnce(timeout, self, RebalancingTimeout)

  peers.foreach(_ ! SolicitRebalancing(id, proposed))
  log.debug("beginning rebalancing with id {}", id)

  when(Soliciting) {

    case Event(result: SolicitRebalancingResult, _) if result.id != id =>
      stay()

    case Event(result: SolicitRebalancingResult, data: Soliciting) =>
      if (result.accepted) {
        log.debug("peer {} accepts solicitation for rebalance id {}", sender(), id)
        val replies = data.replies + sender()
        if (replies.size == peers.size) {
          if (shrinks.nonEmpty)
            goto(Shrinking) using Shrinking(shrinks ++ updates)
          else
            goto(Growing) using Growing(updates ++ grows)
        } else stay() using Soliciting(replies)
      } else {
        log.debug("peer {} refuses solicitation for rebalance id {}", sender(), id)
        context.parent ! RebalancingFails(id)
        stop()
      }

    case Event(RebalancingTimeout, _) =>
      log.debug("rebalancing timed out")
      context.parent ! RebalancingFails(id)
      stop()
  }

  onTransition {
    case Soliciting -> Shrinking =>
      shrinks.head ! ApplyRebalancing(id)
  }

  when(Shrinking) {

    case Event(result: ApplyRebalancingResult, _) if result.id != id =>
      stay()

    case Event(result: ApplyRebalancingResult, data: Shrinking) =>
      log.debug("shrinking peer {} for rebalance id {}", sender(), id)
      if (data.left.length == 1) {
        if (grows.nonEmpty)
          goto(Growing) using Growing(grows)
        else
          goto(Committing) using Committing(Set.empty)
      } else {
        val left = data.left.tail
        left.head ! ApplyRebalancing(id)
        stay() using Shrinking(left)
      }

    case Event(RebalancingTimeout, _) =>
      log.debug("rebalancing timed out")
      context.parent ! RebalancingFails(id)
      stop()
  }

  onTransition {
    case _ -> Growing =>
      grows.head ! ApplyRebalancing(id)
  }

  when(Growing) {

    case Event(result: ApplyRebalancingResult, _) if result.id != id =>
      stay()

    case Event(result: ApplyRebalancingResult, data: Growing) =>
      log.debug("growing peer {} for rebalance id {}", sender(), id)
      if (data.left.length == 1) {
        goto(Committing) using Committing(Set.empty)
      } else {
        val left = data.left.tail
        left.head ! ApplyRebalancing(id)
        stay() using Growing(left)
      }

    case Event(RebalancingTimeout, _) =>
      log.debug("rebalancing timed out")
      context.parent ! RebalancingFails(id)
      stop()
  }

  onTransition {
    case _ -> Committing =>
      peers.foreach(_ ! CommitRebalancing(id))
  }

  when(Committing) {

    case Event(result: CommitRebalancingResult, _) if result.id != id =>
      stay()

    case Event(result: CommitRebalancingResult, data: Committing) =>
      log.debug("peer {} commits rebalance id {}", sender(), id)
      val replies = data.replies + sender()
      if (replies.size == peers.size) {
        log.debug("rebalancing complete for id {}", id)
        context.parent ! RebalancingSucceeds(id)
        stop()
      } else stay() using Committing(replies)

    case Event(RebalancingTimeout, _) =>
      log.debug("rebalancing timed out")
      context.parent ! RebalancingFails(id)
      stop()
  }

  initialize()
}

object ShardRebalancer {
  case object RebalancingTimeout
}

sealed trait ShardRebalancerFSMState
case object Soliciting extends ShardRebalancerFSMState
case object Shrinking extends ShardRebalancerFSMState
case object Growing extends ShardRebalancerFSMState
case object Committing extends ShardRebalancerFSMState

sealed trait ShardRebalancerFSMData
case class Soliciting(replies: Set[ActorRef]) extends ShardRebalancerFSMData
case class Shrinking(left: Vector[ActorRef]) extends ShardRebalancerFSMData
case class Growing(left: Vector[ActorRef]) extends ShardRebalancerFSMData
case class Committing(replies: Set[ActorRef]) extends ShardRebalancerFSMData

case class SolicitRebalancing(id: String, proposed: ShardRing)
case class SolicitRebalancingResult(id: String, accepted: Boolean)

case class ApplyRebalancing(id: String)
case class ApplyRebalancingResult(id: String)

case class CommitRebalancing(id: String)
case class CommitRebalancingResult(id: String)

case class RebalancingSucceeds(id: String)
case class RebalancingFails(id: String)