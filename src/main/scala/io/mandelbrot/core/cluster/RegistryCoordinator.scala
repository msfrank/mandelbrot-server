package io.mandelbrot.core.cluster

import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import io.mandelbrot.core.registry.RegistryServiceCommand

import scala.concurrent.duration.FiniteDuration

/**
 *
 */
class RegistryCoordinator(registryService: ActorRef) extends Actor with ActorLogging {

  def receive = {
    case op: RegistryServiceCommand =>
      registryService forward op
  }
}

object RegistryCoordinator {
  def props(registryService: ActorRef) = Props(classOf[RegistryCoordinator], registryService)
}
