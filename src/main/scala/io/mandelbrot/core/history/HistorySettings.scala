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

import com.typesafe.config.Config
import scala.concurrent.duration.{FiniteDuration, Duration}
import java.io.File
import java.util.concurrent.TimeUnit
import io.mandelbrot.core.ServiceSettings

case class HistorySettings(plugin: String,
                           service: Option[Any],
                           historyRetention: Duration
                           )

object HistorySettings extends ServiceSettings {
  def parse(config: Config): HistorySettings = {
    val historyRetention = FiniteDuration(config.getDuration("history-retention", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    val plugin = config.getString("plugin")
    val service = if (config.hasPath("plugin-settings")) {
      makeServiceSettings(plugin, config.getConfig("plugin-settings"))
    } else None
    new HistorySettings(plugin, service, historyRetention)
  }
}
