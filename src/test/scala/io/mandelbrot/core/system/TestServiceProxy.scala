package io.mandelbrot.core.system

import akka.actor.{ActorLogging, Props, Actor, ActorRef}

import io.mandelbrot.core.history.HistoryServiceOperation
import io.mandelbrot.core.notification.NotificationServiceOperation
import io.mandelbrot.core.registry.RegistryServiceOperation
import io.mandelbrot.core.state.StateServiceOperation
import io.mandelbrot.core.tracking.TrackingServiceOperation

class TestServiceProxy(registryService: Option[ActorRef],
                       trackingService: Option[ActorRef],
                       historyService: Option[ActorRef],
                       notificationService: Option[ActorRef],
                       stateService: Option[ActorRef]) extends Actor with ActorLogging {

  def receive = {

    case op: RegistryServiceOperation =>
      registryService match {
        case Some(ref) =>
          ref forward op
          log.debug("forwarded {}", op)
        case None =>
          log.debug("dropped {}", op)
      }

    case op: ProbeSystemOperation =>
      registryService match {
        case Some(ref) =>
          ref forward op
          log.debug("forwarded {}", op)
        case None =>
          log.debug("dropped {}", op)
      }

    case op: ProbeOperation =>
      registryService match {
        case Some(ref) =>
          ref forward op
          log.debug("forwarded {}", op)
        case None =>
          log.debug("dropped {}", op)
      }

    case op: TrackingServiceOperation =>
      trackingService match {
        case Some(ref) =>
          ref forward op
          log.debug("forwarded {}", op)
        case None =>
          log.debug("dropped {}", op)
      }

    case op: HistoryServiceOperation =>
      historyService match {
        case Some(ref) =>
          ref forward op
          log.debug("forwarded {}", op)
        case None =>
          log.debug("dropped {}", op)
      }

    case op: NotificationServiceOperation =>
      notificationService match {
        case Some(ref) =>
          ref forward op
          log.debug("forwarded {}", op)
        case None =>
          log.debug("dropped {}", op)
      }

    case op: StateServiceOperation =>
      stateService match {
        case Some(ref) =>
          ref forward op
          log.debug("forwarded {}", op)
        case None =>
          log.debug("dropped {}", op)
      }
  }
}

object TestServiceProxy {
  def props(registryService: Option[ActorRef] = None,
            trackingService: Option[ActorRef] = None,
            historyService: Option[ActorRef] = None,
            notificationService: Option[ActorRef] = None,
            stateService: Option[ActorRef] = None) = {
    Props(classOf[TestServiceProxy], registryService, trackingService, historyService, notificationService, stateService)
  }
}
