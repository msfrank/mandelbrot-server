package io.mandelbrot.core.system

import akka.actor.{ActorLogging, Props, Actor, ActorRef}
import io.mandelbrot.core.model.NotificationEvent

import io.mandelbrot.core.registry.RegistryServiceOperation
import io.mandelbrot.core.notification.NotificationServiceOperation
import io.mandelbrot.core.state.StateServiceOperation

class TestServiceProxy(registryService: Option[ActorRef],
                       notificationService: Option[ActorRef],
                       stateService: Option[ActorRef]) extends Actor with ActorLogging {

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

    case unhandled =>
      log.error("received unhandled message {}", unhandled)
  }
}

object TestServiceProxy {
  def props(registryService: Option[ActorRef] = None,
            notificationService: Option[ActorRef] = None,
            stateService: Option[ActorRef] = None) = {
    Props(classOf[TestServiceProxy], registryService, notificationService, stateService)
  }
}
