package io.mandelbrot.core.sequence

import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import org.HdrHistogram.Histogram
import scala.concurrent.duration._

import io.mandelbrot.core.ingest._

/**
 *
 */
class Sequencer(val services: ActorRef, partitionId: String) extends Actor with ActorLogging {

  // config
  val interval = 1.minute
  val limit = 1000

  // state
  var token: Option[String] = None

  override def preStart(): Unit = {
    services ! GetCheckpoint(partitionId)
  }

  def receive = {

    /* */
    case GetCheckpointResult(op, checkpointToken) =>
      token = Some(checkpointToken)
      services ! GetObservations(partitionId, limit, token)

    /* */
    case IngestServiceOperationFailed(op: GetCheckpoint, failure) =>

    /* */
    case GetObservationsResult(op, observations, checkpointToken) =>

    /* */
    case IngestServiceOperationFailed(op, failure) =>
      throw failure
  }
}

object Sequencer {
  def props(services: ActorRef) = Props(classOf[Sequencer], services)
}

case class EpochInterval()