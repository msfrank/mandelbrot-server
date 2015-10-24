/**
 * Copyright 2015 Michael Frank <msfrank@syntaxjockey.com>
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

package io.mandelbrot.core.ingest

import akka.actor.Props
import com.typesafe.config.{ConfigFactory, Config}

import io.mandelbrot.core.ServerConfigException

case class IngestSettings(props: Props)

object IngestSettings {
  def parse(config: Config): IngestSettings = {
    val plugin = config.getString("plugin")
    val pluginSettings = if (config.hasPath("plugin-settings")) config.getConfig("plugin-settings") else ConfigFactory.empty()
    val props = IngestExtension.extensions.get(plugin) match {
      case None =>
        throw new ServerConfigException("%s is not recognized as an IngestExtension".format(plugin))
      case Some(extension) =>
        extension.props(extension.configure(pluginSettings))
    }
    IngestSettings(props)
  }
}
