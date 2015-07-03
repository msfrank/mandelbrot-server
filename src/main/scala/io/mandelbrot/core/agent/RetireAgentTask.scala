package io.mandelbrot.core.agent

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.mandelbrot.core.model.{AgentMetadata, AgentRegistration, AgentSpec}
import io.mandelbrot.core.registry._

/**
 *
 */
class RetireAgentTask(op: RetireAgent,
                      caller: ActorRef,
                      current: AgentRegistration,
                      commit: AgentRegistration,
                      checks: Set[ActorRef],
                      services: ActorRef) extends Actor with ActorLogging {

  var inflight: Set[RegistryServiceOperation] = Set.empty
  var lsn: Long = commit.lsn

  current.spec.groups.diff(commit.spec.groups).foreach { groupName =>
    inflight = inflight + RemoveAgentFromGroup(current.spec.agentId, groupName)
  }
  commit.spec.groups.diff(current.spec.groups).foreach { groupName =>
    inflight = inflight + AddAgentToGroup(commit.metadata, groupName)
  }

  override def preStart(): Unit = {
    if (inflight.nonEmpty)
      services ! PutRegistration(commit.spec.agentId, commit.spec, commit.metadata, lsn)
    else
      services ! PutTombstone(commit.spec.agentId, commit.metadata.generation, commit.metadata.expires.get)
  }

  def receive = {

    case result: PutRegistrationResult =>
      log.debug("put registration for {}", op.agentId)
      checks.foreach(_ ! RetireCheck)
      inflight.foreach(op => services ! op)
      lsn = result.op.lsn + 1

    case result: AddAgentToGroupResult =>
      log.debug("added agent {} to group {}", op.agentId, result.op.groupName)
      inflight = inflight - result.op
      if (inflight.isEmpty) {
        services ! PutTombstone(commit.spec.agentId, commit.metadata.generation, commit.metadata.expires.get)
      }

    case result: RemoveAgentFromGroupResult =>
      log.debug("removed agent {} from group {}", op.agentId, result.op.groupName)
      inflight = inflight - result.op
      if (inflight.isEmpty) {
        services ! PutTombstone(commit.spec.agentId, commit.metadata.generation, commit.metadata.expires.get)
      }

    case result: PutTombstoneResult =>
      log.debug("put tombstone {} for {}:{}", result.op.expires, result.op.agentId, result.op.generation)
      services ! CommitRegistration(commit.spec.agentId, commit.spec, commit.metadata, lsn)

    case result: CommitRegistrationResult =>
      log.debug("committed registration for {}", op.agentId)
      caller ! RetireAgentResult(op, commit.metadata)
      context.parent ! RetireAgentTaskComplete(commit.spec, commit.metadata, lsn)
      context.stop(self)

    case failure: RegistryServiceOperationFailed =>
      throw failure.failure
  }
}

object RetireAgentTask {
  def props(op: RetireAgent,
            caller: ActorRef,
            current: AgentRegistration,
            commit: AgentRegistration,
            checks: Set[ActorRef],
            services: ActorRef) = {
    Props(classOf[RetireAgentTask], op, caller, current, commit, checks, services)
  }
}

case class RetireAgentTaskComplete(registration: AgentSpec, metadata: AgentMetadata, lsn: Long)
