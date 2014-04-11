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

package io.mandelbrot.core.http

import com.typesafe.config.Config
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit

class HttpSettings(val interface: String, val port: Int, val backlog: Int, val requestTimeout: FiniteDuration)

object HttpSettings {
  def parse(config: Config): HttpSettings = {
    val port = config.getInt("port")
    val interface = config.getString("interface")
    val backlog = config.getInt("backlog")
    val requestTimeout = FiniteDuration(config.getDuration("request-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    new HttpSettings(interface, port, backlog, requestTimeout)
  }
}
