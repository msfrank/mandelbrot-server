package io.mandelbrot.core.check

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.pattern.pipe
import io.mandelbrot.core.{ResourceNotFound, ApiException}
import org.joda.time.DateTime
import scala.concurrent.duration._
import scala.collection.mutable

import io.mandelbrot.core.model._
import io.mandelbrot.core.state._

/**
 *
 */
class InitializeCheckTask(checkRef: CheckRef,
                          generation: Long,
                          initializers: Map[ObservationSource,CheckInitializer],
                          caller: ActorRef,
                          services: ActorRef) extends Actor with ActorLogging {
  import context.dispatcher

  // config
  val queryTimeout = 1.second

  // state
  val observations = new mutable.HashMap[ProbeId,Vector[ProbeObservation]]
  var inflight = 0

  override def preStart(): Unit = {
    log.debug("initializing check {}", checkRef)
    initializers.foreach {

      case (source: ObservationSource, query) if source.scheme == "probe" =>
        val op = GetObservationHistory(ProbeRef(checkRef.agentId, ProbeId(source.id)), generation,
          query.from, query.to, query.limit, query.fromInclusive, query.toExclusive, query.descending)
        log.debug("sending query {}", op)
        services.ask(op)(queryTimeout).pipeTo(self)
        inflight = inflight + 1

      case (source, query) =>
        log.error("ignoring query for unknown source {}", source)
    }

    // if there are no queries in flight, then we are done
    if (inflight == 0) {
      caller ! InitializeCheckTaskComplete(Map.empty)
      context.stop(self)
    }
  }

  def receive = {

    case result: GetObservationHistoryResult =>
      log.debug("received history for {}: {}", result.op.probeRef, result.page)
      val history = observations.getOrElse(result.op.probeRef.probeId, Vector.empty[ProbeObservation])
      observations.put(result.op.probeRef.probeId, history ++ result.page.history)
      if (!result.page.exhausted) {
        val op = result.op.copy(last = result.page.last)
        log.debug("sending query {}", op)
        services.ask(op)(queryTimeout).pipeTo(self)
      } else {
        inflight = inflight - 1
        if (inflight == 0) {
          caller ! InitializeCheckTaskComplete(observations.toMap)
          context.stop(self)
        }
      }

    case StateServiceOperationFailed(op: GetObservationHistory, ApiException(ResourceNotFound)) =>
      log.debug("no history found for {}", op.probeRef)
      observations.put(op.probeRef.probeId, Vector.empty)
      inflight = inflight - 1
      if (inflight == 0) {
        caller ! InitializeCheckTaskComplete(observations.toMap)
        context.stop(self)
      }

    case StateServiceOperationFailed(op, failure) =>
      log.debug("failed to get check status: {}", failure)
      caller ! InitializeCheckTaskFailed(failure)
      context.stop(self)
  }

}

object InitializeCheckTask {
  def props(checkRef: CheckRef,
            generation: Long,
            initializers: Map[ObservationSource,CheckInitializer],
            caller: ActorRef,
            services: ActorRef) = {
    Props(classOf[InitializeCheckTask], checkRef, generation, initializers, caller, services)
  }
}

case class CheckInitializer(from: Option[DateTime],
                            to: Option[DateTime],
                            limit: Int,
                            fromInclusive: Boolean,
                            toExclusive: Boolean,
                            descending: Boolean)

case class InitializeCheckTaskComplete(observations: Map[ProbeId,Vector[ProbeObservation]])
case class InitializeCheckTaskFailed(ex: Throwable)