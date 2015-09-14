package io.mandelbrot.persistence.cassandra.task

import akka.actor._
import akka.actor.Status.Failure
import akka.pattern.pipe

import io.mandelbrot.persistence.cassandra._
import io.mandelbrot.persistence.cassandra.dal._
import io.mandelbrot.core.state._
import io.mandelbrot.core.{ApiException,InternalError}

import scala.concurrent.Future

/**
 *
 */
class DeleteCheckStatusTask(op: DeleteStatus,
                            caller: ActorRef,
                            checkStatusIndexDAL: CheckStatusIndexDAL,
                            checkStatusDAL: CheckStatusDAL) extends Actor with ActorLogging {
  import DeleteCheckStatusTask._
  import context.dispatcher

  // bound the amount of epochs we will search in one request
  val maxEpochs = 30

  override def preStart(): Unit = {
    checkStatusIndexDAL.listEpochsInclusiveAscending(op.checkRef, op.generation,
      EpochUtils.SMALLEST_TIMESTAMP, EpochUtils.LARGEST_TIMESTAMP, maxEpochs).pipeTo(self)
  }

  def receive = {

    /* all epochs have been deleted, now delete the index */
    case StatusEpochList(Nil) =>
      log.debug("deleting index for {}:{}", op.checkRef, op.generation)
      checkStatusIndexDAL.deleteIndex(op.checkRef, op.generation)
        .map(_ => DeletedIndex)
        .pipeTo(self)

    /* the next batch of epochs to delete */
    case StatusEpochList(epochs) =>
      val futures = epochs.map { epoch =>
        log.debug("deleting epoch {} for {}:{}", epoch, op.checkRef, op.generation)
        checkStatusDAL.deleteCheckStatus(op.checkRef, op.generation, epoch).map { _ => epoch }
      }
      Future.sequence(futures).map {
        case epochs: List[Long] => DeletedEpochs(epochs)
      }.pipeTo(self)

    /* */
    case DeletedEpochs(epochs) =>
      log.debug("deleted epochs {} for {}:{}", epochs.mkString(","), op.checkRef, op.generation)
      checkStatusIndexDAL.deleteEpochs(op.checkRef, op.generation, epochs)
        .map { _ => DeletedBatch(epochs) }
        .pipeTo(self)

    case DeletedBatch(epochs) =>
      log.debug("removed epochs {} from index for {}:{}", epochs.mkString(","), op.checkRef, op.generation)
      val from = EpochUtils.epoch2timestamp(epochs.sorted.last).plus(1L)
      checkStatusIndexDAL.listEpochsInclusiveAscending(op.checkRef, op.generation,
        from, EpochUtils.LARGEST_TIMESTAMP, maxEpochs).pipeTo(self)

    /* we are done */
    case DeletedIndex =>
      log.debug("deleted index for {}:{}", op.checkRef, op.generation)
      caller ! DeleteStatusResult(op)
      context.stop(self)

    /* we don't know how to handle this exception, let supervisor strategy handle it */
    case Failure(ex: Throwable) =>
      throw ex
  }
}

object DeleteCheckStatusTask {
  def props(op: DeleteStatus,
            caller: ActorRef,
            checkStatusIndexDAL: CheckStatusIndexDAL,
            checkStatusDAL: CheckStatusDAL) = {
    Props(classOf[DeleteCheckStatusTask], op, caller, checkStatusIndexDAL, checkStatusDAL)
  }
  case class DeletedBatch(epochs: List[Long])
  case class DeletedEpochs(epochs: List[Long])
  case object DeletedIndex
}
