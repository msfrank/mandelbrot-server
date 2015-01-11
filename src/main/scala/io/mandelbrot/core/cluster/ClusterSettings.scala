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

import com.typesafe.config.Config
import io.mandelbrot.core.{ServerConfigException, ServiceExtension}
import scala.collection.JavaConversions._

case class CoordinatorSettings(plugin: String, settings: Option[Any])

class ClusterSettings(val enabled: Boolean,
                      val seedNodes: Vector[String],
                      val minNrMembers: Int,
                      val totalShards: Int,
                      val initialWidth: Int,
                      val coordinator: CoordinatorSettings)

object ClusterSettings {
  def parse(config: Config): ClusterSettings = {
    val enabled = config.getBoolean("enabled")
    val seedNodes = config.getStringList("seed-nodes")
    val minNrMembers = config.getInt("min-nr-members")
    val totalShards = config.getInt("total-shards")
    val initialWidth = config.getInt("initial-width")
    val plugin = config.getString("plugin")
    if (!ServiceExtension.pluginImplements(plugin, classOf[Coordinator]))
      throw new ServerConfigException("%s is not recognized as an Coordinator plugin".format(plugin))
    val service = if (config.hasPath("plugin-settings")) {
      ServiceExtension.makePluginSettings(plugin, config.getConfig("plugin-settings"))
    } else None
    new ClusterSettings(enabled, seedNodes.toVector, minNrMembers, totalShards, initialWidth, CoordinatorSettings(plugin, service))
  }
}
