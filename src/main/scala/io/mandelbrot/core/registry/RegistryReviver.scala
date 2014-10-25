package io.mandelbrot.core.registry

import java.net.URI

import akka.actor.{Props, ActorRef, Actor, ActorLogging}
import io.mandelbrot.core.system.ReviveProbeSystem

/**
 *
 */
class RegistryReviver(registryService: ActorRef) extends Actor with ActorLogging {

  var entries = Vector.empty[URI]

  log.debug("reviving registry entries")

  override def preStart(): Unit = {
    registryService ! ListProbeSystems(None, Some(100))
  }

  def receive = {
    case ListProbeSystemsResult(op, systems, last) =>
      systems.keys.foreach(registryService ! ReviveProbeSystem(_))
      context.stop(self)
  }

  override def postStop(): Unit = {
    log.debug("finished reviving registry entries")
  }
}
 
object RegistryReviver {
  def props(registryService: ActorRef) = Props(classOf[RegistryReviver], registryService)
}