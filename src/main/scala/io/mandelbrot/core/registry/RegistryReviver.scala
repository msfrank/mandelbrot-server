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