package io.mandelbrot.core.agent

import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import org.joda.time.DateTime

import io.mandelbrot.core.model.{Observation, ProbeObservationPage, ProbeRef}
import io.mandelbrot.core.{ServiceOperationFailed, ServiceQuery, ServiceCommand, ServiceOperation}

/**
 *
 */
class ProbeManager(observationBus: ObservationBus, services: ActorRef) extends Actor with ActorLogging {

  def receive = {

    /* */
    case op: ProcessProbeObservation =>
      observationBus.publish(op)

    /* */
    case query: GetProbeObservations =>
//      services.ask(GetMetricsHistory(checkRef, generation, query.from, query.to, query.limit,
//        query.fromInclusive, query.toExclusive, query.descending, query.last))(queryTimeout).map {
//        case result: GetMetricsHistoryResult =>
//          GetCheckMetricsResult(query, result.page)
//        case failure: StateServiceOperationFailed =>
//          CheckOperationFailed(query, failure.failure)
//      }.pipeTo(sender())

  }
}

object ProbeManager {
  def props(observationBus: ObservationBus, services: ActorRef) = {
    Props(classOf[ProbeManager], observationBus, services)
  }
}

/* */
sealed trait ProbeOperation extends ServiceOperation
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

