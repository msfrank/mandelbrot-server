package io.mandelbrot.persistence.cassandra.task

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe

import io.mandelbrot.core.state.{StateServiceOperationFailed, UpdateStatus, UpdateStatusResult}
import io.mandelbrot.persistence.cassandra.dal.{CheckStatusIndexDAL, CheckStatusDAL}
import io.mandelbrot.persistence.cassandra.EpochUtils

/**
 * given a CheckRef and the status, persist the status, updating the
 * index if necessary.  If writing to the index or the status tables fails,
 * then give up and pass the exception back to the caller.
 */
class UpdateCheckStatusTask(op: UpdateStatus,
                            caller: ActorRef,
                            checkStatusIndexDAL: CheckStatusIndexDAL,
                            checkStatusDAL: CheckStatusDAL) extends Actor with ActorLogging {
  import UpdateCheckStatusTask._
  import context.dispatcher

  val epoch = EpochUtils.timestamp2epoch(op.status.timestamp)

  override def preStart(): Unit = {
      checkStatusIndexDAL.putEpoch(op.checkRef, op.status.generation, epoch)
        .map(_ => PutEpoch)
        .pipeTo(self)
  }

  def receive = {

    case PutEpoch =>
      checkStatusDAL.updateCheckStatus(op.checkRef, op.status.generation, epoch, op.status, op.notifications)
        .map(_ => PutStatus)
        .pipeTo(self)

    case PutStatus =>
      caller ! UpdateStatusResult(op)

    case Failure(ex: Throwable) =>
      caller ! StateServiceOperationFailed(op, ex)
      context.stop(self)
  }
}

object UpdateCheckStatusTask {
  def props(op: UpdateStatus,
            caller: ActorRef,
            checkStatusIndexDAL: CheckStatusIndexDAL,
            checkStatusDAL: CheckStatusDAL) = {
    Props(classOf[UpdateCheckStatusTask], op, caller, checkStatusIndexDAL, checkStatusDAL)
  }

  case object PutEpoch
  case object PutStatus
}