package io.mandelbrot.persistence.cassandra.task

import akka.actor.Status.Failure
import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import akka.pattern.pipe
import io.mandelbrot.core.{ResourceNotFound, ApiException}
import io.mandelbrot.core.model.{GenerationLsn, RegistrationsPage}

import io.mandelbrot.core.registry.{RegistryManager, GetRegistrationHistoryResult, RegistryServiceOperationFailed, GetRegistrationHistory}
import io.mandelbrot.persistence.cassandra.dal.{AgentRegistrationSnapshot, AgentRegistrationHistory, AgentRegistrationDAL}

/**
 *
 */
class GetAgentRegistrationHistoryTask(op: GetRegistrationHistory,
                                      caller: ActorRef,
                                      agentRegistrationDAL: AgentRegistrationDAL) extends Actor with ActorLogging {
  import context.dispatcher

  // contains the epoch and timestamp where we left off, or None
  val last: Option[GenerationLsn] = extractIterator(op.last)
  val from = last.orElse(op.from)
  val fromInclusive: Boolean = if (last.isDefined) true else op.fromInclusive

  override def preStart(): Unit = {
    // if there are no timeseries parameters, then as a special case return only the last entry
    if (op.from.isEmpty && op.to.isEmpty) {
      agentRegistrationDAL.getAgentRegistrationHistory(op.agentId,
        Some(RegistryManager.MinGenerationLsn), Some(RegistryManager.MaxGenerationLsn),
        limit = 1, fromInclusive = false, toExclusive = false, descending = true).pipeTo(self)
    } else {
      agentRegistrationDAL.getAgentRegistrationHistory(op.agentId, from, op.to,
        op.limit, fromInclusive, op.toExclusive, op.descending).pipeTo(self)
    }
  }

  def receive = {

    /* special case, if no history is found, then return ResourceNotFound */
    case result: AgentRegistrationHistory if result.snapshots.isEmpty =>
      caller ! RegistryServiceOperationFailed(op, ApiException(ResourceNotFound))
      context.stop(self)

    /* return history and stop the actor */
    case result: AgentRegistrationHistory =>
      val registrations = result.snapshots.map(_.registration)
      val page = if (registrations.length == op.limit) {
        RegistrationsPage(registrations, Some(generateIterator(result.snapshots.last)), exhausted = false)
      } else RegistrationsPage(registrations, None, exhausted = true)
      caller ! GetRegistrationHistoryResult(op, page)
      context.stop(self)

    /* if we encounter an unhandled exception, then let the supervision strategy handle it */
    case Failure(ex: Throwable) =>
      throw ex
  }

  /**
   * the client sends the iterator key as an opaque string, so we need to
   * convert it back to a GenerationLsn.
   */
  def extractIterator(last: Option[String]): Option[GenerationLsn] = last.map { string =>
    val parts = string.split("|", 1).map(_.toLong)
    GenerationLsn(parts(0), parts(1))
  }

  /**
   * given a registration snapshot, return the serialized iterator.
   */
  def generateIterator(snapshot: AgentRegistrationSnapshot): String = {
    val generation = snapshot.metadata.generation
    val lsn = snapshot.lsn
    s"$generation|$lsn"
  }
}

object GetAgentRegistrationHistoryTask {
  def props(op: GetRegistrationHistory, caller: ActorRef, agentRegistrationDAL: AgentRegistrationDAL) = {
    Props(classOf[GetAgentRegistrationHistoryTask], op, caller, agentRegistrationDAL)
  }
}