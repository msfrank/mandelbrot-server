package io.mandelbrot.core.system

import akka.actor.{ActorLogging, Props, Actor, ActorRef}

import io.mandelbrot.core.history.HistoryServiceOperation
import io.mandelbrot.core.notification.NotificationManagerOperation
import io.mandelbrot.core.registry.ProbeRegistryOperation
import io.mandelbrot.core.state.StateServiceOperation
import io.mandelbrot.core.tracking.TrackingServiceOperation

class TestServiceProxy(registryService: Option[ActorRef],
                       trackingService: Option[ActorRef],
                       historyService: Option[ActorRef],
                       notificationService: Option[ActorRef],
                       stateService: Option[ActorRef]) extends Actor with ActorLogging {

  def receive = {

    case op: ProbeRegistryOperation =>
      log.debug("received {}", op)
      registryService.foreach(_ forward op)

    case op: TrackingServiceOperation =>
      log.debug("received {}", op)
      trackingService.foreach(_ forward op)

    case op: HistoryServiceOperation =>
      log.debug("received {}", op)
      historyService.foreach(_ forward op)

    case op: NotificationManagerOperation =>
      log.debug("received {}", op)
      notificationService.foreach(_ forward op)

    case op: StateServiceOperation =>
      log.debug("received {}", op)
      stateService.foreach(_ forward op)
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
