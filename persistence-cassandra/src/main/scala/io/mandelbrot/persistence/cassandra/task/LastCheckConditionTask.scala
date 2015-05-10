package io.mandelbrot.persistence.cassandra.task

import akka.actor.Status.Failure
import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.pattern.pipe
import org.joda.time.{DateTime, DateTimeZone}

import io.mandelbrot.core.model.{CheckCondition, CheckConditionPage}
import io.mandelbrot.core.state._
import io.mandelbrot.core.{ApiException, InternalError, ResourceNotFound}
import io.mandelbrot.persistence.cassandra.dal.{EpochList, CheckStatusIndexDAL, CheckConditionHistory, CheckStatusDAL}

/**
 * Given a CheckRef, find the latest status.  Throw ResourceNotFound
 * if the CheckRef doesn't exist.
 */
class LastCheckConditionTask(op: GetConditionHistory,
                              caller: ActorRef,
                              checkStatusIndexDAL: CheckStatusIndexDAL,
                              checkStatusDAL: CheckStatusDAL) extends Actor with ActorLogging {

  override def preStart(): Unit = {
    val initialize = InitializeCheckStatus(op.checkRef, DateTime.now(DateTimeZone.UTC))
    context.actorOf(InitializeCheckStatusTask.props(initialize, self, checkStatusIndexDAL, checkStatusDAL))
  }

  def receive = {

    /* return the newest condition */
    case InitializeCheckStatusResult(_, Some(status)) =>
      val condition = CheckCondition(status.timestamp, status.lifecycle, status.summary, status.health,
        status.correlation, status.acknowledged, status.squelched)
      caller ! GetConditionHistoryResult(op, CheckConditionPage(Vector(condition), last = None, exhausted = true))
      context.stop(self)

    /* there was no status data, so return ResourceNotFound */
    case InitializeCheckStatusResult(_, None) =>
      caller ! StateServiceOperationFailed(op, ApiException(ResourceNotFound))
      context.stop(self)

    /* we received an unexpected error, let supervisor strategy handle it */
    case Failure(ex: Throwable) =>
      throw ex
  }

  /**
   * if we receive an exception, then stop the task and return InternalError
   * to the caller.
   */
  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 1) {
    case ex: Throwable =>
      caller ! StateServiceOperationFailed(op, ApiException(InternalError, ex))
      Stop
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
