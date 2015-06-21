package io.mandelbrot.core.system

import akka.actor.{ActorRef, Props, ActorLogging, Actor}

import io.mandelbrot.core.model.{GenerationLsn, AgentSpec, AgentMetadata}
import io.mandelbrot.core.registry._

/**
 *
 */
class RecoverAgentTask(commit: CommitRegistration, services: ActorRef) extends Actor with ActorLogging {

  var inflight: Set[RegistryServiceOperation] = Set.empty

  override def preStart(): Unit = {
    services ! GetRegistrationHistory(commit.agentId, Some(RegistryManager.MinGenerationLsn),
      Some(GenerationLsn(commit.metadata.generation, commit.lsn)), limit = 1, toExclusive = true,
      descending = true)
  }

  def receive = {

    case result: GetRegistrationHistoryResult =>
      result.page.agents.lastOption match {
        case Some(spec) =>
          spec.groups.diff(commit.registration.groups).foreach { groupName =>
            inflight = inflight + RemoveAgentFromGroup(spec.agentId, groupName)
          }
          commit.registration.groups.diff(spec.groups).foreach { groupName =>
            inflight = inflight + AddAgentToGroup(commit.metadata, groupName)
          }
        case None =>
          inflight = commit.registration.groups.map(AddAgentToGroup(commit.metadata, _))
      }
      if (inflight.isEmpty) {
        log.warning("recovering agent {} but there are no inflight operations", commit.agentId)
        services ! commit
      } else inflight.foreach(op => services ! op)

    case result: AddAgentToGroupResult =>
      inflight = inflight - result.op
      if (inflight.isEmpty) { services ! commit }

    case result: RemoveAgentFromGroupResult =>
      inflight = inflight - result.op
      if (inflight.isEmpty) { services ! commit }

    case result: CommitRegistrationResult =>
      context.parent ! RecoverAgentTaskComplete(commit.registration, commit.metadata, commit.lsn)
      context.stop(self)

    case failure: RegistryServiceOperationFailed =>
      throw failure.failure
  }
}

object RecoverAgentTask {
  def props(commit: CommitRegistration, services: ActorRef) = {
    Props(classOf[RecoverAgentTask], commit, services)
  }
}

case class RecoverAgentTaskComplete(registration: AgentSpec, metadata: AgentMetadata, lsn: Long)
