package io.mandelbrot.persistence.cassandra.task

import akka.actor.Status.Failure
import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.pattern.pipe
import org.joda.time.{DateTime, DateTimeZone}

import io.mandelbrot.core.model.{CheckCondition, CheckConditionPage}
import io.mandelbrot.core.state._
import io.mandelbrot.core.{ApiException, InternalError, ResourceNotFound}
import io.mandelbrot.persistence.cassandra._
import io.mandelbrot.persistence.cassandra.dal.{EpochList, CheckStatusIndexDAL, CheckConditionHistory, CheckStatusDAL}

/**
 * Given a CheckRef, find the latest status.  Throw ResourceNotFound
 * if the CheckRef doesn't exist.
 */
class GetCheckConditionTask(op: GetConditionHistory,
                            caller: ActorRef,
                            checkStatusIndexDAL: CheckStatusIndexDAL,
                            checkStatusDAL: CheckStatusDAL) extends Actor with ActorLogging {
  import context.dispatcher

  // contains the epoch and timestamp where we left off, or None
  val last: Option[DateTime] = extractIterator(op.last)

  // bound the amount of epochs we will search in one request
  val maxEpochs = 30

  // the epochs to search
  var epochs: Vector[Long] = Vector.empty

  // the number of epochs we have found in the index
  var epochsFound = 0

  // the conditions we have found so far
  var conditions: Vector[CheckCondition] = Vector.empty

  override def preStart(): Unit = {
    val from = last.getOrElse(op.from.getOrElse(EpochUtils.SMALLEST_TIMESTAMP)).toDateMidnight.toDateTime(DateTimeZone.UTC)
    val to = op.to.getOrElse(EpochUtils.LARGEST_TIMESTAMP)
    checkStatusIndexDAL.listEpochsInclusiveAscending(op.checkRef, from, to, maxEpochs).pipeTo(self)
  }

  def receive = {

    /* there are no more epochs left to search */
    case EpochList(Nil) =>
      if (conditions.isEmpty) {
        caller ! StateServiceOperationFailed(op, ApiException(ResourceNotFound))
      } else {
        caller ! GetConditionHistoryResult(op, CheckConditionPage(conditions, last = None, exhausted = true))
      }
      context.stop(self)

    /* the list of epochs to search */
    case epochList: EpochList =>
      epochs = epochs ++ epochList.epochs
      epochsFound = epochsFound + epochList.epochs.length
      val epoch = epochs.head
      val from = last.orElse(op.from).map(_.plus(1L))
      val limit = op.limit - conditions.length
      checkStatusDAL.getCheckConditionHistory(op.checkRef, epoch, from, op.to, limit).pipeTo(self)

    /* we have exhausted the current epoch */
    case history: CheckConditionHistory if history.conditions.isEmpty =>
      val epoch = epochs.head
      epochs = epochs.tail
      // we haven't reached the request limit yet, and there are more epochs to search
      if (epochs.nonEmpty) {
        val from = last.orElse(op.from).map(_.plus(1L))
        val limit = op.limit - conditions.length
        checkStatusDAL.getCheckConditionHistory(op.checkRef, epochs.head, from, op.to, limit).pipeTo(self)
      } else if (epochsFound < maxEpochs) {
        // we are confident we have read from all epochs, so this query is exhausted
        caller ! GetConditionHistoryResult(op, CheckConditionPage(conditions, last = None, exhausted = true))
        context.stop(self)
      } else {
        // we have searched the maximum amount of epochs, return what we've got
        val last = conditions.lastOption.map(_.timestamp.getMillis) match {
          case None =>
            epoch.toString
          case Some(timestampMillis) if timestampMillis > epoch =>
            timestampMillis.toString
          case _ =>
            epoch.toString
        }
        caller ! GetConditionHistoryResult(op, CheckConditionPage(conditions, last = Some(last), exhausted = false))
        context.stop(self)
      }

    /* add history to the result */
    case history: CheckConditionHistory =>
      val nleft = op.limit - conditions.length
      if (history.conditions.length < nleft) {
        conditions = conditions ++ history.conditions
        // we can't distinguish a short count from exhausting the epoch, so get
        // from the epoch again and look for a zero count
        val epoch = epochs.head
        val from = Some(conditions.last.timestamp.plus(1L))
        val limit = op.limit - conditions.length
        checkStatusDAL.getCheckConditionHistory(op.checkRef, epoch, from, op.to, limit).pipeTo(self)
      } else {
        // we have reached the request limit, so return what we've got
        conditions = conditions ++ history.conditions.take(nleft)
        val last = conditions.last.timestamp.getMillis.toString
        caller ! GetConditionHistoryResult(op, CheckConditionPage(conditions, last = Some(last), exhausted = false))
        context.stop(self)
      }

    /* */
    case Failure(ex: Throwable) =>
      caller ! StateServiceOperationFailed(op, ex)
      context.stop(self)
  }

  /**
   * the client sends the iterator key as an opaque string, so we need to
   * convert it back to a DateTime.
   */
  def extractIterator(last: Option[String]): Option[DateTime] = last.map { string =>
    EpochUtils.epoch2timestamp(string.toLong)
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

object GetCheckConditionTask {
  def props(op: GetConditionHistory,
            caller: ActorRef,
            checkStatusIndexDAL: CheckStatusIndexDAL,
            checkStatusDAL: CheckStatusDAL) = {
    Props(classOf[GetCheckConditionTask], op, caller, checkStatusIndexDAL, checkStatusDAL)
  }
}
