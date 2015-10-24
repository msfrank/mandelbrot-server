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

package io.mandelbrot.core.state

import akka.actor.Props
import com.typesafe.config.{ConfigFactory, Config}
import scala.concurrent.duration.{FiniteDuration, Duration}
import java.util.concurrent.TimeUnit

import io.mandelbrot.core.ServerConfigException

/**
 *
 */
case class PersisterSettings(plugin: String, settings: Option[Any])

case class StateSettings(maxSummarySize: Long,
                         maxDetailSize: Long,
                         statusHistoryAge: Duration,
                         defaultSearchLimit: Int,
                         props: Props)

object StateSettings {
  def parse(config: Config): StateSettings = {
    val maxSummarySize = config.getBytes("max-summary-size")
    val maxDetailSize = config.getBytes("max-detail-size")
    val statusHistoryAge = FiniteDuration(config.getDuration("status-history-age", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    val defaultSearchLimit = config.getInt("default-search-limit")
    val plugin = config.getString("plugin")
    val pluginSettings = if (config.hasPath("plugin-settings")) config.getConfig("plugin-settings") else ConfigFactory.empty()
    val props = StateExtension.extensions.get(plugin) match {
      case None =>
        throw new ServerConfigException("%s is not recognized as a StateExtension".format(plugin))
      case Some(extension) =>
        extension.props(extension.configure(pluginSettings))
    }
    StateSettings(maxSummarySize,
                  maxDetailSize,
                  statusHistoryAge,
                  defaultSearchLimit,
                  props)
  }
}