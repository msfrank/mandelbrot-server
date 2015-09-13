package io.mandelbrot.core.agent

import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import akka.pattern.ask
import akka.pattern.pipe
import io.mandelbrot.core.check.ProcessObservation
import org.joda.time.DateTime
import scala.concurrent.duration._

import io.mandelbrot.core.model.{Observation, ProbeObservationPage, ProbeRef}
import io.mandelbrot.core.state.{StateServiceOperationFailed, GetObservationHistoryResult, GetObservationHistory}
import io.mandelbrot.core._

/**
 *
 */
class ProbeManager(observationBus: ObservationBus, services: ActorRef) extends Actor with ActorLogging {
  import context.dispatcher

  // config
  val queryTimeout = 1.second

  // state
  var generation: Long = 0

  /*
   * actor starts in unconfigured state.  the actor responds to any operation with
   * RetryLater until it receives the configuration message, at which time it configures
   * and transitions to running state.
   */
  def unconfigured: Receive = {
    case op: ProbeOperation =>
      sender() ! ProbeOperationFailed(op, ApiException(RetryLater))
    case config: SetGeneration =>
      generation = config.generation
      context.become(running)
  }

  def receive = unconfigured

  def running: Receive = {

    /* */
    case op: ProcessProbeObservation =>
      log.debug("received observation {} from {}", op.observation, sender())
      observationBus.publish(ProcessObservation(op.probeRef.probeId, op.observation))
      sender() ! ProcessProbeObservationResult(op)

    /* */
    case query: GetProbeObservations =>
      services.ask(GetObservationHistory(query.probeRef, generation, query.from, query.to, query.limit,
        query.fromInclusive, query.toExclusive, query.descending, query.last))(queryTimeout).map {
        case result: GetObservationHistoryResult =>
          GetProbeObservationsResult(query, result.page)
        case failure: StateServiceOperationFailed =>
          ProbeOperationFailed(query, failure.failure)
      }.pipeTo(sender())

    /* update configuration */
    case config: SetGeneration =>
      generation = config.generation
  }
}

object ProbeManager {
  def props(observationBus: ObservationBus, services: ActorRef) = {
    Props(classOf[ProbeManager], observationBus, services)
  }
}

case class SetGeneration(generation: Long)

/* */
sealed trait ProbeOperation extends ServiceOperation { val probeRef: ProbeRef }
sealed trait ProbeCommand extends ServiceCommand with ProbeOperation
sealed trait ProbeQuery extends ServiceQuery with ProbeOperation
sealed trait ProbeResult
case class ProbeOperationFailed(op: ProbeOperation, failure: Throwable) extends ServiceOperationFailed

case class GetProbeObservations(probeRef: ProbeRef,
                                from: Option[DateTime],
                                to: Option[DateTime],
                                limit: Int,
                                fromInclusive: Boolean,
                                toExclusive: Boolean,
                                descending: Boolean,
                                last: Option[String]) extends ProbeQuery
case class GetProbeObservationsResult(op: GetProbeObservations, page: ProbeObservationPage) extends ProbeResult

case class ProcessProbeObservation(probeRef: ProbeRef, observation: Observation) extends ProbeCommand
case class ProcessProbeObservationResult(op: ProcessProbeObservation) extends ProbeResult

