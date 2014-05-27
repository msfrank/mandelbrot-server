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

package io.mandelbrot.core.tracking

import akka.actor._

/**
 *
 */
class TrackingServiceExtensionImpl(system: ActorSystem) extends Extension {
  val trackingService = system.actorOf(TrackingManager.props(), "tracking-service")
}

/**
 *
 */
object TrackingServiceExtension extends ExtensionId[TrackingServiceExtensionImpl] with ExtensionIdProvider {
  override def lookup() = TrackingServiceExtension
  override def createExtension(system: ExtendedActorSystem) = new TrackingServiceExtensionImpl(system)
  override def get(system: ActorSystem): TrackingServiceExtensionImpl = super.get(system)
}

object TrackingService {
  def apply(system: ActorSystem): ActorRef = TrackingServiceExtension(system).trackingService
}