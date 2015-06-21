package io.mandelbrot.core.agent

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.mandelbrot.core.model.{AgentMetadata, AgentSpec}
import io.mandelbrot.core.registry._
import org.joda.time.{DateTime, DateTimeZone}

/**
 *
 */
class RegisterAgentTask(op: RegisterAgent,
                        caller: ActorRef,
                        generation: Long,
                        lsn: Long,
                        services: ActorRef) extends Actor with ActorLogging {

  val timestamp = DateTime.now(DateTimeZone.UTC)
  val metadata = AgentMetadata(op.agentId, generation, timestamp, timestamp, None)
  var inflight: Set[RegistryServiceOperation] = Set.empty

  override def preStart(): Unit = {
    log.debug("registering agent {}", op.agentId)
    if (op.registration.groups.nonEmpty)
      services ! PutRegistration(op.agentId, op.registration, metadata, lsn)
    else
      services ! CommitRegistration(op.agentId, op.registration, metadata, lsn)
  }

  def receive = {

    case result: PutRegistrationResult =>
      log.debug("registered agent {}: {}", result.op.agentId, result.metadata)
      inflight = op.registration.groups.map {
        groupName => AddAgentToGroup(metadata, groupName)
      }.toSet[RegistryServiceOperation]
      inflight.foreach(op => services ! op)

    case result: AddAgentToGroupResult =>
      log.debug("added agent {} to group {}", op.agentId, result.op.groupName)
      inflight = inflight - result.op
      if (inflight.isEmpty) {
        services ! CommitRegistration(op.agentId, op.registration, metadata, lsn)
      }

    case result: CommitRegistrationResult =>
      caller ! RegisterAgentResult(op, metadata)
      context.parent ! RegisterAgentTaskComplete(op.registration, metadata, lsn)
      context.stop(self)
      log.debug("committed registration for agent {}", op.agentId)

    case failure: RegistryServiceOperationFailed =>
      throw failure.failure
  }
}

object RegisterAgentTask {
  def props(op: RegisterAgent, caller: ActorRef, generation: Long, lsn: Long, services: ActorRef) = {
    Props(classOf[RegisterAgentTask], op, caller, generation, lsn, services)
  }
}

case class RegisterAgentTaskComplete(registration: AgentSpec, metadata: AgentMetadata, lsn: Long)
