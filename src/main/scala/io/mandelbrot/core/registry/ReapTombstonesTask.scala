package io.mandelbrot.core.registry

import akka.actor._
import org.joda.time.DateTime
import scala.concurrent.duration._

import io.mandelbrot.core.model.AgentId
import io.mandelbrot.core.agent.{DeleteAgentResult, AgentOperationFailed, DeleteAgent}

/**
 *
 */
class ReapTombstonesTask(olderThan: DateTime,
                         timeout: FiniteDuration,
                         maxInFlight: Int,
                         services: ActorRef,
                         monitor: ActorRef) extends Actor with ActorLogging {
  import ReapTombstonesTask._
  import context.dispatcher

  // state
  var inflight: Map[AgentGeneration,DateTime] = Map.empty
  var seen = 0
  var deleted = 0
  var taskTimeout: Option[Cancellable] = None

    /* attempt to make some progress */
  override def preStart(): Unit = {
    services ! ListTombstones(olderThan, maxInFlight)
    taskTimeout = Some(context.system.scheduler.scheduleOnce(timeout, self, TaskTimeout))
  }

  def receive = {

    /* there is no more work to do, so signal completion and stop */
    case ListTombstonesResult(_, tombstones) if tombstones.isEmpty && inflight.isEmpty =>
      monitor ! ReaperComplete(seen, deleted)
      context.stop(self)

    /* for each tombstone, delete the corresponding agent generation */
    case ListTombstonesResult(_, tombstones) =>
      val pending: Map[AgentGeneration,DateTime] = tombstones.filterNot {
        tombstone => inflight.contains(AgentGeneration(tombstone.agentId, tombstone.generation))
      }.map {
        tombstone => AgentGeneration(tombstone.agentId, tombstone.generation) -> tombstone.expires
      }.toMap
      pending.keys.foreach {
        case AgentGeneration(agentId, generation) => services ! DeleteAgent(agentId, generation)
      }
      inflight = inflight ++ pending
      seen = seen + pending.size

    /* delete agent succeeded, so delete the tombstone */
    case result: DeleteAgentResult =>
      inflight.get(AgentGeneration(result.op.agentId, result.op.generation)).foreach {
        expires => services ! DeleteTombstone(result.op.agentId, result.op.generation, expires)
      }

    /* delete tombstone succeeded, so we can remove this inflight request */
    case result: DeleteTombstoneResult =>
      inflight = inflight - AgentGeneration(result.op.agentId, result.op.generation)
      deleted = deleted + 1
      log.debug("deleted agent {}:{}", result.op.agentId, result.op.generation)
      // pull more work if it is available
      if (inflight.isEmpty) {
        services ! ListTombstones(olderThan, maxInFlight)
      }

    /* */
    case RegistryServiceOperationFailed(op: ListTombstones, failure) =>
      log.debug("failed to list tombstones: {}", failure)
      monitor ! ReaperComplete(seen, deleted)
      context.stop(self)

    /* */
    case AgentOperationFailed(op: DeleteAgent, failure) =>
      inflight = inflight - AgentGeneration(op.agentId, op.generation)
      log.debug("failed to delete agent: {}", failure)

    /* */
    case RegistryServiceOperationFailed(op: DeleteTombstone, failure) =>
      inflight = inflight - AgentGeneration(op.agentId, op.generation)
      log.debug("failed to delete tombstone: {}", failure)

    /* we have run for the maximum allotted time */
    case TaskTimeout =>
      taskTimeout = None
      monitor ! ReaperComplete(seen, deleted)
      context.stop(self)
  }

  override def postStop(): Unit = taskTimeout.foreach(_.cancel())
}

object ReapTombstonesTask {
  def props(olderThan: DateTime,
            timeout: FiniteDuration,
            maxInFlight: Int,
            services: ActorRef,
            monitor: ActorRef) = {
    Props(classOf[ReapTombstonesTask], olderThan, timeout, maxInFlight, services, monitor)
  }
  case object TaskTimeout
  case class AgentGeneration(agentId: AgentId, generation: Long)
}

case class ReaperComplete(seen: Int, deleted: Int)
