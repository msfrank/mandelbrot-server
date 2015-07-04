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

import akka.actor.Props
import com.typesafe.config.{ConfigFactory, Config}
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit

import io.mandelbrot.core.ServerConfigException

case class PolicyDefaults(joiningTimeout: Option[FiniteDuration],
                          checkTimeout: Option[FiniteDuration],
                          alertTimeout: Option[FiniteDuration],
                          leavingTimeout: Option[FiniteDuration])

case class RegistrySettings(policyMin: PolicyDefaults,
                            policyMax: PolicyDefaults,
                            reaperInterval: FiniteDuration,
                            reaperTimeout: FiniteDuration,
                            maxDeletesInFlight: Int,
                            clusterRole: Option[String],
                            maxHandOverRetries: Int,
                            maxTakeOverRetries: Int,
                            retryInterval: FiniteDuration,
                            props: Props)

object RegistrySettings {
  def parse(config: Config): RegistrySettings = {
    val policyMin = {
      val joiningTimeoutMin = if (!config.hasPath("min-joining-timeout")) None else {
        Some(FiniteDuration(config.getDuration("min-joining-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      }
      val checkTimeoutMin = if (!config.hasPath("min-check-timeout")) None else {
        Some(FiniteDuration(config.getDuration("min-check-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      }
      val alertTimeoutMin = if (!config.hasPath("min-alert-timeout")) None else {
        Some(FiniteDuration(config.getDuration("min-alert-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      }
      val leavingTimeoutMin = if (!config.hasPath("min-leaving-timeout")) None else {
        Some(FiniteDuration(config.getDuration("min-leaving-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      }
      PolicyDefaults(joiningTimeoutMin, checkTimeoutMin, alertTimeoutMin, leavingTimeoutMin)
    }
    val policyMax = {
      val joiningTimeoutMax = if (!config.hasPath("max-joining-timeout")) None else {
        Some(FiniteDuration(config.getDuration("max-joining-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      }
      val checkTimeoutMax = if (!config.hasPath("max-check-timeout")) None else {
        Some(FiniteDuration(config.getDuration("max-check-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      }
      val alertTimeoutMax = if (!config.hasPath("max-alert-timeout")) None else {
        Some(FiniteDuration(config.getDuration("max-alert-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      }
      val leavingTimeoutMax = if (!config.hasPath("max-leaving-timeout")) None else {
        Some(FiniteDuration(config.getDuration("max-leaving-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      }
      PolicyDefaults(joiningTimeoutMax, checkTimeoutMax, alertTimeoutMax, leavingTimeoutMax)
    }
    val reaperInterval = FiniteDuration(config.getDuration("reaper-interval",
      TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    val reaperTimeout = FiniteDuration(config.getDuration("reaper-timeout",
      TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    val maxDeletesInFlight = config.getInt("max-deletes-in-flight")
    val clusterRole: Option[String] = if (config.hasPath("reaper-cluster-role")) Some(config.getString("reaper-cluster-role")) else None
    val maxHandOverRetries = config.getInt("reaper-handover-retries")
    val maxTakeOverRetries = config.getInt("reaper-takeover-retries")
    val retryInterval = FiniteDuration(config.getDuration("reaper-retry-interval",
      TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    val plugin = config.getString("plugin")
    val pluginSettings = if (config.hasPath("plugin-settings")) config.getConfig("plugin-settings") else ConfigFactory.empty()
    val props = RegistryPersister.extensions.get(plugin) match {
      case None =>
        throw new ServerConfigException("%s is not recognized as a RegistryPersisterExtension".format(plugin))
      case Some(extension) =>
        extension.props(extension.configure(pluginSettings))
    }
    RegistrySettings(policyMin, policyMax, reaperInterval, reaperTimeout,
      maxDeletesInFlight, clusterRole, maxHandOverRetries, maxTakeOverRetries,
      retryInterval, props)
  }
}

