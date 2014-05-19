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

import com.typesafe.config.{ConfigObject, ConfigValueType, Config}
import scala.collection.JavaConversions._

import io.mandelbrot.core.ServiceExtension

case class NotifierSettings(plugin: String, settings: Option[Any])

case class NotificationSettings(notifiers: Map[String,NotifierSettings])

object NotificationSettings {
  def parse(config: Config): NotificationSettings = {
    val notifiers = config.getConfig("notifiers").root.flatMap {
      case (name,configValue) if configValue.valueType() == ConfigValueType.OBJECT =>
        val notifierConfig = configValue.asInstanceOf[ConfigObject].toConfig
        val plugin = notifierConfig.getString("plugin")
        val settings = if (config.hasPath("plugin-settings")) {
          ServiceExtension.makeServiceSettings(plugin, config.getConfig("plugin-settings"))
        } else None
        Some(name -> NotifierSettings(plugin, settings))
      case unknown =>
        None
    }.toMap
    new NotificationSettings(notifiers)
  }
}

/* */
sealed trait NotificationPolicy
case object EmitNotificationPolicy extends NotificationPolicy
case object EscalateNotificationPolicy extends NotificationPolicy
case object SquelchNotificationPolicy extends NotificationPolicy
