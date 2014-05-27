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

import com.typesafe.config.Config

import io.mandelbrot.core.ServiceExtension

/**
 *
 */
case class TrackerSettings(plugin: String, settings: Option[Any])

case class TrackingSettings(tracker: TrackerSettings)

object TrackingSettings {
  def parse(config: Config): TrackingSettings = {
    val plugin = config.getString("plugin")
    val service = if (config.hasPath("plugin-settings")) {
      ServiceExtension.makePluginSettings(plugin, config.getConfig("plugin-settings"))
    } else None
    TrackingSettings(TrackerSettings(plugin, service))
  }
}