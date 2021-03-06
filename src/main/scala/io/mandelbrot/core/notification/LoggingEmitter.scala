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

import akka.actor.{Props, ActorLogging, Actor}
import com.typesafe.config.Config

import io.mandelbrot.core.model._

/**
 *
 */
class LoggingEmitter(settings: LoggingEmitterSettings) extends Actor with ActorLogging {

  def receive = {

    case NotifyContact(contact, notification) =>
      log.debug("notify {} <= {}", contact, notification)
  }
}

object LoggingEmitter {
  def props(settings: LoggingEmitterSettings) = Props(classOf[LoggingEmitter], settings)
  def settings(config: Config, unused: Any): Option[Any] = None
}

case class LoggingEmitterSettings()

class LoggingNotificationEmitter extends NotificationEmitterExtension {
  type Settings = LoggingEmitterSettings
  def configure(config: Config): Settings = LoggingEmitterSettings()
  def props(settings: Settings): Props = LoggingEmitter.props(settings)
}