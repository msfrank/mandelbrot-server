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

package io.mandelbrot.core.registry

import com.typesafe.config.{ConfigFactory, ConfigObject, Config}
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import java.net.URI
import java.io.File
import java.util.concurrent.TimeUnit

import io.mandelbrot.core.notification.{SquelchNotificationPolicy, EscalateNotificationPolicy, EmitNotificationPolicy}
import io.mandelbrot.core.ServiceSettings

case class RegistrySettings(plugin: String,
                            service: Option[Any],
                            defaultPolicy: ProbePolicy)

object RegistrySettings extends ServiceSettings {
  def parse(config: Config): RegistrySettings = {
    val defaultPolicy: ProbePolicy = {
      val joiningTimeout = FiniteDuration(config.getDuration("joining-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
      val probeTimeout = FiniteDuration(config.getDuration("probe-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
      val alertTimeout = FiniteDuration(config.getDuration("alert-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
      val leavingTimeout = FiniteDuration(config.getDuration("leaving-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
      val flapWindow = FiniteDuration(config.getDuration("flap-window", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
      val flapDeviations = config.getInt("flap-deviations")
      val notificationPolicyType = config.getString("notification-policy") match {
        case "emit" => EmitNotificationPolicy
        case "escalate" => EscalateNotificationPolicy
        case "squelch" => SquelchNotificationPolicy
        case unknown => throw new IllegalArgumentException()
      }
      ProbePolicy(joiningTimeout, probeTimeout, alertTimeout, leavingTimeout, flapWindow, flapDeviations, notificationPolicyType)
    }
    val plugin = config.getString("plugin")
    val service = if (config.hasPath("plugin-settings")) {
      makeServiceSettings(plugin, config.getConfig("plugin-settings"))
    } else None
    new RegistrySettings(plugin, service, defaultPolicy)
  }
}

