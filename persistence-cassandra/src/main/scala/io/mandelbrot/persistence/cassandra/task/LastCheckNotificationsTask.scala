package io.mandelbrot.persistence.cassandra.task

import akka.actor._
import akka.actor.SupervisorStrategy.Stop
import akka.actor.Status.Failure
import akka.pattern.pipe
import io.mandelbrot.core.model.{CheckNotificationsPage, CheckNotifications}
import org.joda.time.{DateTime, DateTimeZone}

import io.mandelbrot.core.state._
import io.mandelbrot.persistence.cassandra.EpochUtils
import io.mandelbrot.core.{ApiException, InternalError, ResourceNotFound}
import io.mandelbrot.persistence.cassandra.dal.{CheckStatusIndexDAL, CheckStatusDAL}

/**
 * Given a CheckRef, find the latest status.  Throw ResourceNotFound
 * if the CheckRef doesn't exist.
 */
class LastCheckNotificationsTask(op: GetNotificationsHistory,
                                 caller: ActorRef,
                                 checkStatusIndexDAL: CheckStatusIndexDAL,
                                 checkStatusDAL: CheckStatusDAL) extends Actor with ActorLogging {
  import context.dispatcher

  override def preStart(): Unit = {
    val initialize = InitializeCheckStatus(op.checkRef, DateTime.now(DateTimeZone.UTC))
    context.actorOf(InitializeCheckStatusTask.props(initialize, self, checkStatusIndexDAL, checkStatusDAL))
  }

  def receive = {

    /* use the timestamp from the status to look up the latest notifications */
    case InitializeCheckStatusResult(_, Some(status)) =>
      val epoch = EpochUtils.timestamp2epoch(status.timestamp)
      checkStatusDAL.getCheckNotifications(op.checkRef, epoch, status.timestamp)
        .recover { case ApiException(ResourceNotFound) => CheckNotifications(status.timestamp, Vector.empty) }
        .pipeTo(self)

    /* return the newest notifications */
    case notifications: CheckNotifications =>
      caller ! GetNotificationsHistoryResult(op, CheckNotificationsPage(Vector(notifications), last = None, exhausted = true))
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

object LastCheckNotificationsTask {
  def props(op: GetNotificationsHistory,
            caller: ActorRef,
            checkStatusIndexDAL: CheckStatusIndexDAL,
            checkStatusDAL: CheckStatusDAL) = {
    Props(classOf[LastCheckNotificationsTask], op, caller, checkStatusIndexDAL, checkStatusDAL)
  }
}
