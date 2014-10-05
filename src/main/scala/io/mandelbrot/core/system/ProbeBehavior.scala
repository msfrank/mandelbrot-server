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

package io.mandelbrot.core.system

import io.mandelbrot.core.notification.Notification
import io.mandelbrot.core.system.Probe.{ProbeExpiryTimeout, ProbeAlertTimeout}

import scala.concurrent.Future

/**
 *
 */
trait ProbeBehavior {
  def makeProbeBehavior(): ProbeBehaviorInterface
}

/**
 *
 */
trait ProbeBehaviorInterface {
  def enter(probe: ProbeInterface): Option[ProbeMutation]
  def update(probe: ProbeInterface, policy: ProbeBehavior): Option[ProbeMutation]
  def processStatus(probe: ProbeInterface, message: StatusMessage): Option[ProbeMutation]
  def processMetrics(probe: ProbeInterface, message: MetricsMessage): Option[ProbeMutation]
  def processChild(probe: ProbeInterface, message: ProbeStatus): Option[ProbeMutation]
  def processAlertTimeout(probe: ProbeInterface): Option[ProbeMutation]
  def processExpiryTimeout(probe: ProbeInterface): Option[ProbeMutation]
  def retire(probe: ProbeInterface, lsn: Long): Option[ProbeMutation]
  def exit(probe: ProbeInterface): Option[ProbeMutation]

  def process(probe: ProbeInterface, message: Any): Option[ProbeMutation] = message match {
    case m: StatusMessage => processStatus(probe, m)
    case m: MetricsMessage => processMetrics(probe, m)
    case m: ProbeStatus => processChild(probe, m)
    case ProbeAlertTimeout => processAlertTimeout(probe)
    case ProbeExpiryTimeout => processExpiryTimeout(probe)
    case _ => throw new IllegalArgumentException()
  }
}
