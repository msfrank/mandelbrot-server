package io.mandelbrot.persistence.cassandra.task

import akka.actor._
import akka.actor.Status.Failure
import akka.pattern.pipe
import org.joda.time.{DateTime, DateTimeZone}

import io.mandelbrot.core.state.GetMetricsHistory
import io.mandelbrot.persistence.cassandra.dal.{EpochList, CheckMetricsHistory, CheckStatusDAL, CheckStatusIndexDAL}
import io.mandelbrot.core.model.{CheckMetrics, CheckMetricsPage}
import io.mandelbrot.core.state._
import io.mandelbrot.core.{ApiException, InternalError, ResourceNotFound}
import io.mandelbrot.persistence.cassandra._

/**
 * Given a CheckRef, find the latest status.  Throw ResourceNotFound
 * if the CheckRef doesn't exist.
 */
class GetCheckMetricsTask(op: GetMetricsHistory,
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
  var metrics: Vector[CheckMetrics] = Vector.empty

  override def preStart(): Unit = {
    val from = last.getOrElse(op.from.getOrElse(EpochUtils.SMALLEST_TIMESTAMP)).toDateMidnight.toDateTime(DateTimeZone.UTC)
    val to = op.to.getOrElse(EpochUtils.LARGEST_TIMESTAMP)
    checkStatusIndexDAL.listEpochsInclusiveAscending(op.checkRef, from, to, maxEpochs).pipeTo(self)
  }

  def receive = {

    /* there are no more epochs left to search */
    case EpochList(Nil) =>
      if (metrics.isEmpty) {
        caller ! StateServiceOperationFailed(op, ApiException(ResourceNotFound))
      } else {
        caller ! GetMetricsHistoryResult(op, CheckMetricsPage(metrics, last = None, exhausted = true))
      }
      context.stop(self)

    /* the list of epochs to search */
    case epochList: EpochList =>
      epochs = epochs ++ epochList.epochs
      epochsFound = epochsFound + epochList.epochs.length
      val epoch = epochs.head
      val from = last.orElse(op.from).map(_.plus(1L))
      val limit = op.limit - metrics.length
      checkStatusDAL.getCheckMetricsHistory(op.checkRef, epoch, from, op.to, limit,
        !op.fromInclusive, !op.toExclusive, op.descending).pipeTo(self)

    /* we have exhausted the current epoch */
    case history: CheckMetricsHistory if history.metrics.isEmpty =>
      val epoch = epochs.head
      epochs = epochs.tail
      // we haven't reached the request limit yet, and there are more epochs to search
      if (epochs.nonEmpty) {
        val from = last.orElse(op.from).map(_.plus(1L))
        val limit = op.limit - metrics.length
        checkStatusDAL.getCheckMetricsHistory(op.checkRef, epochs.head, from, op.to, limit,
          !op.fromInclusive, !op.toExclusive, op.descending).pipeTo(self)
      } else if (epochsFound < maxEpochs) {
        // we are confident we have read from all epochs, so this query is exhausted
        caller ! GetMetricsHistoryResult(op, CheckMetricsPage(metrics, last = None, exhausted = true))
        context.stop(self)
      } else {
        // we have searched the maximum amount of epochs, return what we've got
        val last = metrics.lastOption.map(_.timestamp.getMillis) match {
          case None =>
            epoch.toString
          case Some(timestampMillis) if timestampMillis > epoch =>
            timestampMillis.toString
          case _ =>
            epoch.toString
        }
        caller ! GetMetricsHistoryResult(op, CheckMetricsPage(metrics, last = Some(last), exhausted = false))
        context.stop(self)
      }

    /* add history to the result */
    case history: CheckMetricsHistory =>
      val nleft = op.limit - metrics.length
      if (history.metrics.length < nleft) {
        metrics = metrics ++ history.metrics
        // we can't distinguish a short count from exhausting the epoch, so get
        // from the epoch again and look for a zero count
        val epoch = epochs.head
        val from = Some(metrics.last.timestamp.plus(1L))
        val limit = op.limit - metrics.length
        checkStatusDAL.getCheckMetricsHistory(op.checkRef, epoch, from, op.to, limit,
          !op.fromInclusive, !op.toExclusive, op.descending).pipeTo(self)
      } else {
        // we have reached the request limit, so return what we've got
        metrics = metrics ++ history.metrics.take(nleft)
        val last = metrics.last.timestamp.getMillis.toString
        caller ! GetMetricsHistoryResult(op, CheckMetricsPage(metrics, last = Some(last), exhausted = false))
        context.stop(self)
      }

    /* */
    case Failure(ex: Throwable) =>
      throw ex
  }

  /**
   * the client sends the iterator key as an opaque string, so we need to
   * convert it back to a DateTime.
   */
  def extractIterator(last: Option[String]): Option[DateTime] = last.map { string =>
    EpochUtils.epoch2timestamp(string.toLong)
  }
}

object GetCheckMetricsTask {
  def props(op: GetMetricsHistory,
            caller: ActorRef,
            checkStatusIndexDAL: CheckStatusIndexDAL,
            checkStatusDAL: CheckStatusDAL) = {
    Props(classOf[GetCheckMetricsTask], op, caller, checkStatusIndexDAL, checkStatusDAL)
  }
}
