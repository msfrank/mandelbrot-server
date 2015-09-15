package io.mandelbrot.persistence.cassandra.task

import akka.actor._
import akka.actor.SupervisorStrategy.Stop
import akka.actor.Status.Failure
import org.joda.time.{DateTime, DateTimeZone}

import io.mandelbrot.core.state._
import io.mandelbrot.core.model.{CheckCondition, CheckConditionPage}
import io.mandelbrot.core.{ApiException, InternalError, ResourceNotFound}
import io.mandelbrot.persistence.cassandra.dal.{CheckStatusIndexDAL, CheckStatusDAL}

/**
 * Given a CheckRef, find the latest status.  Throw ResourceNotFound
 * if the CheckRef doesn't exist.
 */
class LastCheckConditionTask(op: GetConditionHistory,
                             caller: ActorRef,
                             checkStatusIndexDAL: CheckStatusIndexDAL,
                             checkStatusDAL: CheckStatusDAL) extends Actor with ActorLogging {

  override def preStart(): Unit = {
    val initialize = GetStatus(op.checkRef, op.generation)
    context.actorOf(InitializeCheckStatusTask.props(initialize, self, checkStatusIndexDAL, checkStatusDAL))
  }

  def receive = {

    /* return the newest condition */
    case GetStatusResult(_, Some(status)) =>
      val condition = CheckCondition(op.generation, status.timestamp, status.lifecycle,
        status.summary, status.health, status.correlation, status.acknowledged, status.squelched)
      caller ! GetConditionHistoryResult(op, CheckConditionPage(Vector(condition), last = None, exhausted = true))
      context.stop(self)

    /* there was no status data, so return ResourceNotFound */
    case GetStatusResult(_, None) =>
      caller ! StateServiceOperationFailed(op, ApiException(ResourceNotFound))
      context.stop(self)

    /* we received an unexpected error, let supervisor strategy handle it */
    case Failure(ex: Throwable) =>
      throw ex
  }
}

object LastCheckConditionTask {
  def props(op: GetConditionHistory,
            caller: ActorRef,
            checkStatusIndexDAL: CheckStatusIndexDAL,
            checkStatusDAL: CheckStatusDAL) = {
    Props(classOf[LastCheckConditionTask], op, caller, checkStatusIndexDAL, checkStatusDAL)
  }
}
