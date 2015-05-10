package io.mandelbrot.persistence.cassandra

import akka.actor.Status.Failure
import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import akka.pattern.pipe

import io.mandelbrot.core.state.{UpdateCheckStatusResult, StateServiceOperationFailed, UpdateCheckStatus}
import io.mandelbrot.persistence.cassandra.UpdateCheckStatusTask.PutStatus

/**
 * given a CheckRef and the status, persist the status, updating the
 * index if necessary.  If writing to the index or the status tables fails,
 * then give up and pass the exception back to the caller.
 */
class UpdateCheckStatusTask(op: UpdateCheckStatus,
                            caller: ActorRef,
                            checkStatusIndexDAL: CheckStatusIndexDAL,
                            checkStatusDAL: CheckStatusDAL) extends Actor with ActorLogging {
  import UpdateCheckStatusTask.PutEpoch
  import context.dispatcher

  val epoch = EpochUtils.timestamp2epoch(op.status.timestamp)

  override def preStart(): Unit = {
      checkStatusIndexDAL.putEpoch(op.checkRef, epoch)
        .map(_ => PutEpoch)
        .pipeTo(self)
  }

  def receive = {

    case PutEpoch =>
      checkStatusDAL.updateCheckStatus(op.checkRef, epoch, op.status, op.notifications)
        .map(_ => PutStatus)
        .pipeTo(self)

    case PutStatus =>
      caller ! UpdateCheckStatusResult(op)

    case Failure(ex: Throwable) =>
      caller ! StateServiceOperationFailed(op, ex)
      context.stop(self)
  }
}

object UpdateCheckStatusTask {
  def props(op: UpdateCheckStatus,
            caller: ActorRef,
            checkStatusIndexDAL: CheckStatusIndexDAL,
            checkStatusDAL: CheckStatusDAL) = {
    Props(classOf[UpdateCheckStatusTask], op, caller, checkStatusIndexDAL, checkStatusDAL)
  }

  case object PutEpoch
  case object PutStatus
}