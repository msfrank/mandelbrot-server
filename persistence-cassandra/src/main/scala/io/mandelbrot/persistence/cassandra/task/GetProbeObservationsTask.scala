package io.mandelbrot.persistence.cassandra.task

import akka.actor._
import akka.actor.Status.Failure
import akka.pattern.pipe
import org.joda.time.{DateTime, DateTimeZone}

import io.mandelbrot.persistence.cassandra.dal._
import io.mandelbrot.core.model.{ProbeObservation, ProbeObservationPage}
import io.mandelbrot.core.state._
import io.mandelbrot.core.{ApiException, InternalError, ResourceNotFound}
import io.mandelbrot.persistence.cassandra._

/**
 * Given a CheckRef, find the latest status.  Throw ResourceNotFound
 * if the CheckRef doesn't exist.
 */
class GetProbeObservationsTask(op: GetObservationHistory,
                               caller: ActorRef,
                               probeObservationIndexDAL: ProbeObservationIndexDAL,
                               probeObservationDAL: ProbeObservationDAL) extends Actor with ActorLogging {
  import context.dispatcher

  // contains the epoch and timestamp where we left off, or None
  val last: Option[DateTime] = extractIterator(op.last)

  // bound the amount of epochs we will search in one request
  val maxEpochs = 30

  // the epochs to search
  var epochs: Vector[Long] = Vector.empty

  // the number of epochs we have found in the index
  var epochsFound = 0

  // the observations we have found so far
  var observations: Vector[ProbeObservation] = Vector.empty

  override def preStart(): Unit = {
    val from = last.getOrElse(op.from.getOrElse(EpochUtils.SMALLEST_TIMESTAMP))
      .toDateMidnight.toDateTime(DateTimeZone.UTC)
    val to = op.to.getOrElse(EpochUtils.LARGEST_TIMESTAMP)
    val f = if (op.descending)
      probeObservationIndexDAL.listEpochsInclusiveDescending(op.probeRef, op.generation, from, to, maxEpochs)
     else
      probeObservationIndexDAL.listEpochsInclusiveAscending(op.probeRef, op.generation, from, to, maxEpochs)
    f.pipeTo(self)
  }

  def receive = {

    /* there are no more epochs left to search */
    case ObservationEpochList(Nil) =>
      if (observations.isEmpty) {
        caller ! StateServiceOperationFailed(op, ApiException(ResourceNotFound))
      } else {
        caller ! GetObservationHistoryResult(op, ProbeObservationPage(observations, last = None, exhausted = true))
      }
      context.stop(self)

    /* the list of epochs to search */
    case epochList: ObservationEpochList =>
      epochs = epochs ++ epochList.epochs
      epochsFound = epochsFound + epochList.epochs.length
      val epoch = epochs.head
      val from = last.orElse(op.from).map(from => if (op.descending) from.minus(1L) else from.plus(1L))
      val limit = op.limit - observations.length
      probeObservationDAL.getProbeObservationHistory(op.probeRef, op.generation, epoch,
        from, op.to, limit, !op.fromInclusive, !op.toExclusive, op.descending).pipeTo(self)

    /* we have exhausted the current epoch */
    case history: ProbeObservationHistory if history.observations.isEmpty =>
      val epoch = epochs.head
      epochs = epochs.tail
      // we haven't reached the request limit yet, and there are more epochs to search
      if (epochs.nonEmpty) {
        val limit = op.limit - observations.length
        val (from: Option[DateTime],to: Option[DateTime]) = if (op.descending) {
          (op.from, Some(observations.last.observation.timestamp.minus(1L)))
        } else (Some(observations.last.observation.timestamp.plus(1L)), op.to)
        probeObservationDAL.getProbeObservationHistory(op.probeRef, op.generation, epochs.head,
          from, to, limit, !op.fromInclusive, !op.toExclusive, op.descending).pipeTo(self)
      } else if (epochsFound < maxEpochs) {
        // we are confident we have read from all epochs, so this query is exhausted
        caller ! GetObservationHistoryResult(op, ProbeObservationPage(observations, last = None, exhausted = true))
        context.stop(self)
      } else {
        // we have searched the maximum amount of epochs, return what we've got
        val last = observations.lastOption.map(_.observation.timestamp.getMillis) match {
          case None =>
            epoch.toString
          case Some(timestampMillis) if timestampMillis > epoch =>
            timestampMillis.toString
          case _ =>
            epoch.toString
        }
        caller ! GetObservationHistoryResult(op, ProbeObservationPage(observations, last = Some(last), exhausted = false))
        context.stop(self)
      }

    /* add history to the result */
    case history: ProbeObservationHistory =>
      val nleft = op.limit - observations.length
      if (history.observations.length < nleft) {
        observations = observations ++ history.observations
        // we can't distinguish a short count from exhausting the epoch, so get
        // from the epoch again and look for a zero count
        val epoch = epochs.head
        val limit = op.limit - observations.length
        val (from: Option[DateTime],to: Option[DateTime]) = if (op.descending) {
          (op.from, Some(observations.last.observation.timestamp.minus(1L)))
        } else (Some(observations.last.observation.timestamp.plus(1L)), op.to)
        probeObservationDAL.getProbeObservationHistory(op.probeRef, op.generation, epoch,
          from, to, limit, !op.fromInclusive, !op.toExclusive, op.descending).pipeTo(self)
      } else {
        // we have reached the request limit, so return what we've got
        observations = observations ++ history.observations.take(nleft)
        val last = observations.last.observation.timestamp.getMillis.toString
        caller ! GetObservationHistoryResult(op, ProbeObservationPage(observations, last = Some(last), exhausted = false))
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

object GetProbeObservationsTask {
  def props(op: GetObservationHistory,
            caller: ActorRef,
            probeObservationIndexDAL: ProbeObservationIndexDAL,
            probeObservationDAL: ProbeObservationDAL) = {
    Props(classOf[GetProbeObservationsTask], op, caller, probeObservationIndexDAL, probeObservationDAL)
  }
}
