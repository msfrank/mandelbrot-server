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

package io.mandelbrot.core.history

import akka.actor._
import org.slf4j.LoggerFactory

import io.mandelbrot.core.{ServiceExtension, ServerConfig}

/**
 *
 */
class HistoryServiceExtensionImpl(system: ActorSystem) extends Extension {
  val historyService = system.actorOf(HistoryManager.props(), "history-service")
}

/**
 *
 */
object HistoryServiceExtension extends ExtensionId[HistoryServiceExtensionImpl] with ExtensionIdProvider {
  override def lookup() = HistoryServiceExtension
  override def createExtension(system: ExtendedActorSystem) = new HistoryServiceExtensionImpl(system)
  override def get(system: ActorSystem): HistoryServiceExtensionImpl = super.get(system)
}

object HistoryService {
  def apply(system: ActorSystem): ActorRef = HistoryServiceExtension(system).historyService
}