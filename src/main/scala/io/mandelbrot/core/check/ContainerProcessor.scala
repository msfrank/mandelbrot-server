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

package io.mandelbrot.core.check

import org.joda.time.{DateTimeZone, DateTime}
import scala.util.{Try, Failure}

import io.mandelbrot.core.{BadRequest, ApiException}
import io.mandelbrot.core.model._

case class ContainerCheckSettings()

/**
 *
 */
class ContainerProcessor(settings: ContainerCheckSettings) extends BehaviorProcessor {

  def initialize(check: AccessorOps): InitializeEffect = InitializeEffect(Map.empty)

  def configure(check: AccessorOps, observations: Map[ProbeId, Vector[ProbeObservation]], children: Set[CheckRef]): ConfigureEffect = {
    val initial = {
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val status = check.getCheckStatus(timestamp)
      status.copy(lifecycle = CheckSynthetic, health = CheckUnknown, lastUpdate = Some(timestamp), lastChange = Some(timestamp))
    }
    ConfigureEffect(initial, Vector.empty, Set.empty)
  }

  def processObservation(check: AccessorOps, probeId: ProbeId, observation: Observation): Option[EventEffect] = None

  def processChild(check: AccessorOps, childRef: CheckRef, childStatus: CheckStatus): Option[EventEffect] = None

  def processExpiryTimeout(check: AccessorOps): Option[EventEffect] = None

  def processAlertTimeout(check: AccessorOps): Option[EventEffect] = None


}

class ContainerCheck extends CheckBehaviorExtension {
  type Settings = ContainerCheckSettings
  class ContainerProcessorFactory(val settings: ContainerCheckSettings) extends DependentProcessorFactory {
    def implement() = new ContainerProcessor(settings)
    val observes = Set.empty[ProbeId]
  }
  def configure(properties: Map[String,String]) = {
    new ContainerProcessorFactory(ContainerCheckSettings())
  }
}
