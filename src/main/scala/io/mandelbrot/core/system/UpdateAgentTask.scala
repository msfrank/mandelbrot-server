package io.mandelbrot.core.system

import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import org.joda.time.{DateTimeZone, DateTime}

import io.mandelbrot.core.model.{AgentRegistration, AgentSpec, AgentMetadata}
import io.mandelbrot.core.registry._

/**
 *
 */
class UpdateAgentTask(op: UpdateAgent,
                      caller: ActorRef,
                      current: AgentRegistration,
                      commit: AgentRegistration,
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
      inflight.foreach(op => services ! op)
      lsn = result.op.lsn + 1

    case result: AddAgentToGroupResult =>
      inflight = inflight - result.op
      if (inflight.isEmpty) {
        services ! CommitRegistration(commit.spec.agentId, commit.spec, commit.metadata, lsn)
      }

    case result: RemoveAgentFromGroupResult =>
      inflight = inflight - result.op
      if (inflight.isEmpty) {
        services ! CommitRegistration(commit.spec.agentId, commit.spec, commit.metadata, lsn)
      }

    case result: CommitRegistrationResult =>
      caller ! UpdateAgentResult(op, commit.metadata)
      context.parent ! UpdateAgentTaskComplete(commit.spec, commit.metadata, lsn)
      context.stop(self)

    case failure: RegistryServiceOperationFailed =>
      throw failure.failure
  }
}

object UpdateAgentTask {
  def props(op: UpdateAgent,
            caller: ActorRef,
            current: AgentRegistration,
            commit: AgentRegistration,
            services: ActorRef) = {
    Props(classOf[UpdateAgentTask], op, caller, current, commit, services)
  }
}

case class UpdateAgentTaskComplete(registration: AgentSpec, metadata: AgentMetadata, lsn: Long)
