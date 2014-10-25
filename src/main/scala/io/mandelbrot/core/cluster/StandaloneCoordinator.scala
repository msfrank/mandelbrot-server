package io.mandelbrot.core.cluster

import akka.actor._
import scala.collection.mutable
import java.net.URI

import io.mandelbrot.core.registry.RegistryServiceOperation
import io.mandelbrot.core.system.{ProbeSystem, ProbeSystemOperation, ProbeOperation}

/**
 *
 */
class StandaloneCoordinator(registryService: ActorRef) extends Actor with ActorLogging {
  
  val systemEntries = new mutable.HashMap[URI,ActorRef]()
  val entryUris = new mutable.HashMap[ActorRef, URI]()

  log.info("server is running in standalone mode")

  def receive = {
    
    case op: RegistryServiceOperation =>
      registryService forward op

    case op: ProbeSystemOperation =>
      val entry = systemEntries.get(op.uri) match {
        case Some(actorRef) => actorRef
        case None =>
          val actorRef = context.actorOf(ProbeSystem.props(context.parent))
          context.watch(actorRef)
          systemEntries.put(op.uri, actorRef)
          entryUris.put(actorRef, op.uri)
          actorRef
      }
      entry forward op

    case op: ProbeOperation =>
      val entry = systemEntries.get(op.probeRef.uri) match {
        case Some(actorRef) => actorRef
        case None =>
          val actorRef = context.actorOf(ProbeSystem.props(context.parent))
          context.watch(actorRef)
          systemEntries.put(op.probeRef.uri, actorRef)
          entryUris.put(actorRef, op.probeRef.uri)
          actorRef
      }
      entry forward op

    case Terminated(actorRef) =>
      entryUris.remove(actorRef) match {
        case Some(uri) => systemEntries.remove(uri)
        case None =>
      }
  }
}

object StandaloneCoordinator {
  def props(registryService: ActorRef) = Props(classOf[StandaloneCoordinator], registryService)
}
