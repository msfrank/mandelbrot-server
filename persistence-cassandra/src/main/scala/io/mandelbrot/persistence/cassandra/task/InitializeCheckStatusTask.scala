package io.mandelbrot.persistence.cassandra.task

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import io.mandelbrot.core.model.CheckStatus
import io.mandelbrot.core.state._
import io.mandelbrot.core.{ApiException, ResourceNotFound}
import io.mandelbrot.persistence.cassandra.dal.{EpochList, CheckStatusIndexDAL, CheckStatusDAL}
import io.mandelbrot.persistence.cassandra.EpochUtils

/**
 * Given a CheckRef, find the latest status.  Throw ResourceNotFound
 * if the CheckRef doesn't exist.
 */
class InitializeCheckStatusTask(op: GetStatus,
                                caller: ActorRef,
                                checkStatusIndexDAL: CheckStatusIndexDAL,
                                checkStatusDAL: CheckStatusDAL) extends Actor with ActorLogging {
  import InitializeCheckStatusTask.EmptyEpoch
  import context.dispatcher

  // bound the amount of epochs we will search in one request
  val maxEpochs = 30

  var epochs: List[Long] = List.empty

  override def preStart(): Unit = {
    checkStatusIndexDAL.getLastEpoch(op.checkRef, op.generation).map {
      case epoch: Long => EpochList(List(epoch))
    }.recover {
      case ApiException(ResourceNotFound) => EpochList(List.empty)
    }.pipeTo(self)
  }

  def receive = {

    case EpochList(Nil) =>
      caller ! GetStatusResult(op, None)
      context.stop(self)

    case epochList: EpochList =>
      val epoch = epochList.epochs.head
      epochs = epochList.epochs.tail
      checkStatusDAL.getLastCheckStatus(op.checkRef, op.generation, epoch).recover {
        case ApiException(ResourceNotFound) => EmptyEpoch(epoch)
      }.pipeTo(self)

    case emptyEpoch: EmptyEpoch =>
      if (epochs.nonEmpty) {
        val epoch = epochs.head
        epochs = epochs.tail
        checkStatusDAL.getLastCheckStatus(op.checkRef, op.generation, epoch).recover {
          case ApiException(ResourceNotFound) => EmptyEpoch(epoch)
        }.pipeTo(self)
      } else {
        checkStatusIndexDAL.listEpochsInclusiveDescending(op.checkRef, op.generation,
          EpochUtils.SMALLEST_TIMESTAMP, EpochUtils.epoch2timestamp(emptyEpoch.epoch - 1), maxEpochs).pipeTo(self)
      }

    case checkStatus: CheckStatus =>
      caller ! GetStatusResult(op, Some(checkStatus))
      context.stop(self)

    case Failure(ex: Throwable) =>
      caller ! StateServiceOperationFailed(op, ex)
      context.stop(self)
  }
}

object InitializeCheckStatusTask {
  def props(op: GetStatus,
            caller: ActorRef,
            checkStatusIndexDAL: CheckStatusIndexDAL,
            checkStatusDAL: CheckStatusDAL) = {
    Props(classOf[InitializeCheckStatusTask], op, caller, checkStatusIndexDAL, checkStatusDAL)
  }
  case class EmptyEpoch(epoch: Long)
}

