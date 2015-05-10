package io.mandelbrot.persistence.cassandra.task

import akka.actor._
import akka.actor.SupervisorStrategy.Stop
import akka.actor.Status.Failure
import org.joda.time.{DateTime, DateTimeZone}

import io.mandelbrot.core.state._
import io.mandelbrot.core.state.GetMetricsHistory
import io.mandelbrot.core.model.{CheckMetrics, CheckMetricsPage}
import io.mandelbrot.core.{ResourceNotFound, InternalError, ApiException}
import io.mandelbrot.persistence.cassandra.dal.{CheckStatusDAL, CheckStatusIndexDAL}


/**
 * Given a CheckRef, find the latest status.  Throw ResourceNotFound
 * if the CheckRef doesn't exist.
 */
class LastCheckMetricsTask(op: GetMetricsHistory,
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
      val metrics = CheckMetrics(status.timestamp, status.metrics)
      caller ! GetMetricsHistoryResult(op, CheckMetricsPage(Vector(metrics), last = None, exhausted = true))
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

object LastCheckMetricsTask {
  def props(op: GetMetricsHistory,
            caller: ActorRef,
            checkStatusIndexDAL: CheckStatusIndexDAL,
            checkStatusDAL: CheckStatusDAL) = {
    Props(classOf[LastCheckMetricsTask], op, caller, checkStatusIndexDAL, checkStatusDAL)
  }
}
