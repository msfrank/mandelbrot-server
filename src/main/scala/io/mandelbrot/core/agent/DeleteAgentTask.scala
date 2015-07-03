package io.mandelbrot.core.agent

import akka.actor._
import scala.concurrent.duration.FiniteDuration

import io.mandelbrot.core.model.{CheckRef, GenerationLsn}
import io.mandelbrot.core.registry._
import io.mandelbrot.core.state._
import io.mandelbrot.core.{RetryLater, ApiException}

/**
 *
 */
class DeleteAgentTask(op: DeleteAgent,
                      caller: ActorRef,
                      timeout: FiniteDuration,
                      services: ActorRef) extends Actor with ActorLogging {
  import DeleteAgentTask._
  import context.dispatcher

  // config
  val limit = 100   // max registry history items to retrieve in a single call
  val getRegistrationHistory = GetRegistrationHistory(op.agentId,
      from = Some(GenerationLsn(op.generation, 0)),
      to = Some(GenerationLsn(op.generation, Long.MaxValue)),
      limit, fromInclusive = true)

  // state
  val checkRefsPending = new java.util.HashSet[CheckRef]
  val checkRefsDeleted = new java.util.HashSet[CheckRef]
  var last: Option[String] = None
  var exhausted: Boolean = false
  var taskTimeout: Option[Cancellable] = None

  log.debug("deleting agent {}:{}", op.agentId, op.generation)

  override def preStart(): Unit = {
    services ! getRegistrationHistory
    taskTimeout = Some(context.system.scheduler.scheduleOnce(timeout, self, TaskTimeout))
  }

  def receive = {

    /* */
    case result: GetRegistrationHistoryResult if result.page.agents.isEmpty =>
      log.debug("last = {}, exhausted = {}", result.page.last, result.page.exhausted)
      last = result.page.last
      exhausted = result.page.exhausted
      if (exhausted)
        services ! DeleteRegistration(op.agentId, op.generation)
      else
        services ! getRegistrationHistory.copy(last = last)

    /* */
    case result: GetRegistrationHistoryResult =>
      log.debug("retrieved {} registration snapshots for {}:{}", result.page.agents.length, op.agentId, op.generation)
      log.debug("last = {}, exhausted = {}", result.page.last, result.page.exhausted)
      last = result.page.last
      exhausted = result.page.exhausted
      result.page.agents
        .flatMap(_.checks.keys.map(CheckRef(op.agentId, _)))
        .filterNot(checkRefsDeleted.contains)
        .foreach {
          case checkRef if !checkRefsPending.contains(checkRef) =>
            services ! DeleteStatus(checkRef, op.generation)
            log.debug("deleting check {}:{}", checkRef, op.generation)
            checkRefsPending.add(checkRef)
          case _ => // do nothing
        }
      if (checkRefsPending.isEmpty)
        services ! getRegistrationHistory.copy(last = last)

    /* we have deleted check data for the specified generation */
    case result: DeleteStatusResult =>
      log.debug("deleted check {}:{}", result.op.checkRef, result.op.generation)
      checkRefsPending.remove(result.op.checkRef)
      checkRefsDeleted.add(result.op.checkRef)
      if (checkRefsPending.isEmpty) {
        if (exhausted)
          services ! DeleteRegistration(op.agentId, op.generation)
        else
          services ! getRegistrationHistory.copy(last = last)
      }

    /* we have deleted all agent data, return success to the caller and stop */
    case result: DeleteRegistrationResult =>
      log.debug("deleted agent {}:{}", op.agentId, op.generation)
      caller ! DeleteAgentResult(op)
      context.stop(self)

    /* failed to delete check data, retry */
    case StateServiceOperationFailed(op: DeleteStatus, failure) =>
      services ! op

    /* failed to get registration history for the agent, retry */
    case RegistryServiceOperationFailed(op: GetRegistrationHistory, failure) =>
      services ! getRegistrationHistory.copy(last = last)

    /* failed to delete registrations, retry */
    case RegistryServiceOperationFailed(op: DeleteRegistration, failure) =>
      services ! op

    /* we exceeded the timeout, give up and return error to the caller */
    case TaskTimeout =>
      taskTimeout = None
      caller ! AgentOperationFailed(op, ApiException(RetryLater))
      context.stop(self)
  }

  override def postStop(): Unit = taskTimeout.foreach(_.cancel())
}

object DeleteAgentTask {
  def props(op: DeleteAgent, caller: ActorRef, timeout: FiniteDuration, services: ActorRef) = {
    Props(classOf[DeleteAgentTask], op, caller, timeout, services)
  }

  case object TaskTimeout
}
