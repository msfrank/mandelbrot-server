package io.mandelbrot.persistence.cassandra.task

import akka.actor._
import akka.actor.Status.Failure
import akka.pattern.pipe

import io.mandelbrot.core.registry.{DeleteRegistrationResult, DeleteRegistration}
import io.mandelbrot.persistence.cassandra.dal.AgentRegistrationDAL

/**
 *
 */
class DeleteAgentRegistrationTask(op: DeleteRegistration,
                                  caller: ActorRef,
                                  agentRegistrationDAL: AgentRegistrationDAL) extends Actor with ActorLogging {
  import DeleteAgentRegistrationTask._
  import context.dispatcher

  // bound the amount of registrations we will delete simultaneously
  val maxInflight = 100

  override def preStart(): Unit = {
    agentRegistrationDAL.listAgentRegistrationGenerationLsns(op.agentId, op.generation, maxInflight)
      .map(LsnList)
      .pipeTo(self)
  }

  def receive = {

    /* all lsns have been deleted, we are done */
    case LsnList(Nil) =>
      caller ! DeleteRegistrationResult(op)
      context.stop(self)

    /* the next batch of lsns to delete */
    case LsnList(lsns) =>
      log.debug("deleting {} registrations for {}:{}", lsns.length, op.agentId, op.generation)
      agentRegistrationDAL.deleteAgentRegistration(op.agentId, op.generation, lsns)
        .map(_ => DeletedLsns(lsns))
        .pipeTo(self)

    /* lsns have been deleted, fetch the next batch */
    case DeletedLsns(lsns) =>
      log.debug("deleted lsns {} for {}:{}", lsns.mkString(","), op.agentId, op.generation)
      agentRegistrationDAL.listAgentRegistrationGenerationLsns(op.agentId, op.generation, maxInflight)
        .map(LsnList)
        .pipeTo(self)

    /* we don't know how to handle this exception, let supervisor strategy handle it */
    case Failure(ex: Throwable) =>
      throw ex
  }
}

object DeleteAgentRegistrationTask {
  def props(op: DeleteRegistration,
            caller: ActorRef,
            agentRegistrationDAL: AgentRegistrationDAL) = {
    Props(classOf[DeleteAgentRegistrationTask], op, caller, agentRegistrationDAL)
  }
  case class LsnList(lsns: List[Long])
  case class DeletedLsns(lsns: List[Long])
}
