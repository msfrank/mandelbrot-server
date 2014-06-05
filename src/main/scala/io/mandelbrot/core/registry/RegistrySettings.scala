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

import com.typesafe.config.Config
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit

case class PolicyDefaults(joiningTimeout: Option[FiniteDuration],
                          probeTimeout: Option[FiniteDuration],
                          alertTimeout: Option[FiniteDuration],
                          leavingTimeout: Option[FiniteDuration])

case class RegistrySettings(policyMin: PolicyDefaults,
                            policyMax: PolicyDefaults,
                            snapshotInitialDelay: FiniteDuration,
                            snapshotInterval: FiniteDuration)

object RegistrySettings {
  def parse(config: Config): RegistrySettings = {
    val policyMin = {
      val joiningTimeoutMin = if (!config.hasPath("min-joining-timeout")) None else {
        Some(FiniteDuration(config.getDuration("min-joining-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      }
      val probeTimeoutMin = if (!config.hasPath("min-probe-timeout")) None else {
        Some(FiniteDuration(config.getDuration("min-probe-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      }
      val alertTimeoutMin = if (!config.hasPath("min-alert-timeout")) None else {
        Some(FiniteDuration(config.getDuration("min-alert-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      }
      val leavingTimeoutMin = if (!config.hasPath("min-leaving-timeout")) None else {
        Some(FiniteDuration(config.getDuration("min-leaving-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      }
      PolicyDefaults(joiningTimeoutMin, probeTimeoutMin, alertTimeoutMin, leavingTimeoutMin)
    }
    val policyMax = {
      val joiningTimeoutMax = if (!config.hasPath("max-joining-timeout")) None else {
        Some(FiniteDuration(config.getDuration("max-joining-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      }
      val probeTimeoutMax = if (!config.hasPath("max-probe-timeout")) None else {
        Some(FiniteDuration(config.getDuration("max-probe-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      }
      val alertTimeoutMax = if (!config.hasPath("max-alert-timeout")) None else {
        Some(FiniteDuration(config.getDuration("max-alert-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      }
      val leavingTimeoutMax = if (!config.hasPath("max-leaving-timeout")) None else {
        Some(FiniteDuration(config.getDuration("max-leaving-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      }
      PolicyDefaults(joiningTimeoutMax, probeTimeoutMax, alertTimeoutMax, leavingTimeoutMax)
    }
    val snapshotInitialDelay = FiniteDuration(config.getDuration("snapshot-initial-delay", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    val snapshotInterval = FiniteDuration(config.getDuration("snapshot-interval", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    new RegistrySettings(policyMin, policyMax, snapshotInitialDelay, snapshotInterval)
  }
}

