package io.mandelbrot.core.system

import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import org.joda.time.{DateTimeZone, DateTime}

import io.mandelbrot.core.model.{AgentRegistration, AgentSpec, AgentMetadata}
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
      services ! CommitRegistration(commit.spec.agentId, commit.spec, commit.metadata, lsn)
  }

  def receive = {

    case result: PutRegistrationResult =>
      checks.foreach(_ ! RetireCheck)
      inflight.foreach(op => services ! op)
      lsn = result.op.lsn + 1

    case result: AddAgentToGroupResult =>
      inflight = inflight - result.op
      if (inflight.isEmpty) {
        services ! PutTombstone(commit.spec.agentId, commit.metadata.generation, commit.metadata.expires.get)
      }

    case result: RemoveAgentFromGroupResult =>
      inflight = inflight - result.op
      if (inflight.isEmpty) {
        services ! PutTombstone(commit.spec.agentId, commit.metadata.generation, commit.metadata.expires.get)
      }

    case result: PutTombstoneResult =>
      services ! CommitRegistration(commit.spec.agentId, commit.spec, commit.metadata, lsn)

    case result: CommitRegistrationResult =>
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
