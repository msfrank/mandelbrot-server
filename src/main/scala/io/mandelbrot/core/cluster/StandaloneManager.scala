/**
 * Copyright 2014 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Mandelbrot.
 *
 * Mandelbrot is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Mandelbrot is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Mandelbrot.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mandelbrot.core.cluster

import akka.actor._
import scala.collection.mutable
import java.net.URI

import io.mandelbrot.core.registry.RegistryServiceOperation
import io.mandelbrot.core.system.{ProbeSystem, ProbeSystemOperation, ProbeOperation}

/**
 *
 */
class StandaloneManager(registryService: ActorRef) extends Actor with ActorLogging {
  
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

object StandaloneManager {
  def props(registryService: ActorRef) = Props(classOf[StandaloneManager], registryService)
}
