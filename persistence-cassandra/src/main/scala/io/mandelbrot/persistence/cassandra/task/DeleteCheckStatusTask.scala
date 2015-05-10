package io.mandelbrot.persistence.cassandra.task

import akka.actor._
import akka.actor.SupervisorStrategy.Stop
import akka.actor.Status.Failure
import akka.pattern.pipe

import io.mandelbrot.core.state._
import io.mandelbrot.core.{ApiException,InternalError}
import io.mandelbrot.persistence.cassandra._
import io.mandelbrot.persistence.cassandra.dal.{EpochList, CheckStatusIndexDAL, CheckStatusDAL}

import scala.concurrent.Future

/**
 * Given a CheckRef, find the latest status.  Throw ResourceNotFound
 * if the CheckRef doesn't exist.
 */
class DeleteCheckStatusTask(op: DeleteCheckStatus,
                            caller: ActorRef,
                            checkStatusIndexDAL: CheckStatusIndexDAL,
                            checkStatusDAL: CheckStatusDAL) extends Actor with ActorLogging {
  import DeleteCheckStatusTask._
  import context.dispatcher

  val to = op.until.map { timestamp =>
    val epoch = EpochUtils.timestamp2epoch(timestamp)
    val prev = EpochUtils.prevEpoch(epoch)
    EpochUtils.epoch2timestamp(prev)
  }.getOrElse(EpochUtils.LARGEST_TIMESTAMP)

  // bound the amount of epochs we will search in one request
  val maxEpochs = 30

  override def preStart(): Unit = {
    checkStatusIndexDAL.listEpochsInclusiveAscending(op.checkRef,
      EpochUtils.SMALLEST_TIMESTAMP, to, maxEpochs).pipeTo(self)
  }

  def receive = {

    /* all epochs have been deleted, now delete the index */
    case EpochList(Nil) =>
      if (op.until.nonEmpty) {
        log.debug("deleting index ")
        checkStatusIndexDAL.deleteIndex(op.checkRef).map(_ => DeletedIndex).pipeTo(self)
      } else {
        caller ! DeleteCheckStatusResult(op)
        context.stop(self)
      }

    /* the next batch of epochs to delete */
    case EpochList(epochs) =>
      val futures = epochs.map { epoch =>
        log.debug("deleting epoch {}", epoch)
        checkStatusDAL.deleteCheckStatus(op.checkRef, epoch).map { _ => epoch }
      }
      Future.sequence(futures).map {
        case epochs: List[Long] => DeletedEpochs(epochs)
      }.pipeTo(self)

    /* */
    case DeletedEpochs(epochs) =>
      log.debug("deleted epochs {}", epochs)
      checkStatusIndexDAL.deleteEpochs(op.checkRef, epochs)
        .map { _ => DeletedBatch(epochs) }
        .pipeTo(self)

    case DeletedBatch(epochs) =>
      log.debug("updated index")
      val from = EpochUtils.epoch2timestamp(epochs.sorted.last).plus(1L)
      checkStatusIndexDAL.listEpochsInclusiveAscending(op.checkRef,
        from, to, maxEpochs).pipeTo(self)

    /* we are done */
    case DeletedIndex =>
      log.debug("deleted index")
      caller ! DeleteCheckStatusResult(op)
      context.stop(self)

    /* we don't know how to handle this exception, let supervisor strategy handle it */
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

object DeleteCheckStatusTask {
  def props(op: DeleteCheckStatus,
            caller: ActorRef,
            checkStatusIndexDAL: CheckStatusIndexDAL,
            checkStatusDAL: CheckStatusDAL) = {
    Props(classOf[DeleteCheckStatusTask], op, caller, checkStatusIndexDAL, checkStatusDAL)
  }
  case class DeletedBatch(epochs: List[Long])
  case class DeletedEpochs(epochs: List[Long])
  case object DeletedIndex
}
