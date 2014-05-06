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

package io.mandelbrot.core.notification

import akka.actor._

import io.mandelbrot.core.{ServerConfig, ServiceExtension}
import org.slf4j.LoggerFactory

/**
 *
 */
class NotificationServiceExtensionImpl(system: ActorSystem) extends ServiceExtension {
  val notificationService = {
    val settings = ServerConfig(system).settings.notifications
    val plugin = settings.plugin
    val service = settings.service
    LoggerFactory.getLogger("io.mandelbrot.core.notification.NotificationServiceExtension").info("loading plugin " + plugin)
    system.actorOf(makeServiceProps(plugin, service), "notification-service")
  }
}

/**
 *
 */
object NotificationServiceExtension extends ExtensionId[NotificationServiceExtensionImpl] with ExtensionIdProvider {
  override def lookup() = NotificationServiceExtension
  override def createExtension(system: ExtendedActorSystem) = new NotificationServiceExtensionImpl(system)
  override def get(system: ActorSystem): NotificationServiceExtensionImpl = super.get(system)
}

object NotificationService {
  def apply(system: ActorSystem): ActorRef = NotificationServiceExtension(system).notificationService
}
