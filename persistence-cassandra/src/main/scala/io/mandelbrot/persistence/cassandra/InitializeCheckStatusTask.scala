package io.mandelbrot.persistence.cassandra

import akka.actor.{Props, ActorRef, Actor, ActorLogging}
import akka.actor.Status.Failure
import akka.pattern.pipe

import io.mandelbrot.core.model.CheckStatus
import io.mandelbrot.core.state._
import io.mandelbrot.core.{ResourceNotFound, ApiException}

/**
 * Given a CheckRef, find the latest status.  Throw ResourceNotFound
 * if the CheckRef doesn't exist.
 */
class InitializeCheckStatusTask(op: InitializeCheckStatus,
                                caller: ActorRef,
                                checkStatusIndexDAL: CheckStatusIndexDAL,
                                checkStatusDAL: CheckStatusDAL) extends Actor with ActorLogging {
  import InitializeCheckStatusTask.EmptyEpoch
  import context.dispatcher

  var epochs: List[Long] = List.empty

  override def preStart(): Unit = {
    checkStatusIndexDAL.getLastEpoch(op.checkRef).map {
      case epoch: Long => EpochList(List(epoch))
    }.recover {
      case ApiException(ResourceNotFound) => EpochList(List.empty)
    }.pipeTo(self)
  }

  def receive = {

    case EpochList(Nil) =>
      caller ! InitializeCheckStatusResult(op, None)
      context.stop(self)

    case epochList: EpochList =>
      val epoch = epochList.epochs.head
      epochs = epochList.epochs.tail
      checkStatusDAL.getLastCheckStatus(op.checkRef, epoch).recover {
        case ApiException(ResourceNotFound) => EmptyEpoch(epoch)
      }.pipeTo(self)

    case emptyEpoch: EmptyEpoch =>
      if (epochs.nonEmpty) {
        val epoch = epochs.head
        epochs = epochs.tail
        checkStatusDAL.getLastCheckStatus(op.checkRef, epoch).recover {
          case ApiException(ResourceNotFound) => EmptyEpoch(epoch)
        }.pipeTo(self)
      } else {
        checkStatusIndexDAL.listEpochsInclusiveDescending(op.checkRef, EpochUtils.SMALLEST_TIMESTAMP,
          EpochUtils.epoch2timestamp(emptyEpoch.epoch - 1), limit = 7).pipeTo(self)
      }

    case checkStatus: CheckStatus =>
      caller ! InitializeCheckStatusResult(op, Some(checkStatus))
      context.stop(self)

    case Failure(ex: Throwable) =>
      caller ! StateServiceOperationFailed(op, ex)
      context.stop(self)
  }
}

object InitializeCheckStatusTask {
  def props(op: InitializeCheckStatus,
            caller: ActorRef,
            checkStatusIndexDAL: CheckStatusIndexDAL,
            checkStatusDAL: CheckStatusDAL) = {
    Props(classOf[InitializeCheckStatusTask], op, caller, checkStatusIndexDAL, checkStatusDAL)
  }
  case class EmptyEpoch(epoch: Long)
}

