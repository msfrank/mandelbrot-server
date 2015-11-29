package io.mandelbrot.core

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.mandelbrot.core.agent.AgentOperation
import io.mandelbrot.core.check.CheckOperation
import io.mandelbrot.core.ingest.IngestServiceOperation
import io.mandelbrot.core.metrics.MetricsServiceOperation
import io.mandelbrot.core.model.NotificationEvent
import io.mandelbrot.core.notification.NotificationServiceOperation
import io.mandelbrot.core.registry.RegistryServiceOperation
import io.mandelbrot.core.state.StateServiceOperation

class TestServiceProxy(registryService: Option[ActorRef],
                       notificationService: Option[ActorRef],
                       stateService: Option[ActorRef],
                       ingestService: Option[ActorRef],
                       metricsService: Option[ActorRef]) extends Actor with ActorLogging {

  def receive = {

    case op: RegistryServiceOperation =>
      registryService match {
        case Some(ref) =>
          ref forward op
          log.debug("forwarded {} to {}", op, ref)
        case None =>
          log.debug("dropped {}", op)
      }

    case op: AgentOperation =>
      registryService match {
        case Some(ref) =>
          ref forward op
          log.debug("forwarded {} to {}", op, ref)
        case None =>
          log.debug("dropped {}", op)
      }

    case op: CheckOperation =>
      registryService match {
        case Some(ref) =>
          ref forward op
          log.debug("forwarded {} to {}", op, ref)
        case None =>
          log.debug("dropped {}", op)
      }

    case op: NotificationServiceOperation =>
      notificationService match {
        case Some(ref) =>
          ref forward op
          log.debug("forwarded {} to {}", op, ref)
        case None =>
          log.debug("dropped {}", op)
      }

    case event: NotificationEvent =>
      notificationService match {
        case Some(ref) =>
          ref forward event
          log.debug("forwarded {} to {}", event, ref)
        case None =>
          log.debug("dropped {}", event)
      }

    case op: StateServiceOperation =>
      stateService match {
        case Some(ref) =>
          ref forward op
          log.debug("forwarded {} to {}", op, ref)
        case None =>
          log.debug("dropped {}", op)
      }

    case op: IngestServiceOperation =>
      ingestService match {
        case Some(ref) =>
          ref forward op
          log.debug("forwarded {} to {}", op, ref)
        case None =>
          log.debug("dropped {}", op)
      }

    case op: MetricsServiceOperation =>
      metricsService match {
        case Some(ref) =>
          ref forward op
          log.debug("forwarded {} to {}", op, ref)
        case None =>
          log.debug("dropped {}", op)
      }

    case unhandled =>
      log.error("received unhandled message {}", unhandled)
  }
}

object TestServiceProxy {
  def props(registryService: Option[ActorRef] = None,
            notificationService: Option[ActorRef] = None,
            stateService: Option[ActorRef] = None,
            ingestService: Option[ActorRef] = None,
            metricsService: Option[ActorRef] = None) = {
    Props(classOf[TestServiceProxy], registryService, notificationService,
      stateService, ingestService, metricsService)
  }
}
