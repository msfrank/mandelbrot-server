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

import org.joda.time.{DateTimeZone, DateTime}
import scala.util.{Try, Failure}

import io.mandelbrot.core.{BadRequest, ApiException}
import io.mandelbrot.core.model._

case class ContainerCheckSettings()

/**
 *
 */
class ContainerProcessor(settings: ContainerCheckSettings) extends BehaviorProcessor {

  def initialize(): InitializeEffect = InitializeEffect(None)

  def configure(status: ProbeStatus, children: Set[ProbeRef]): ConfigureEffect = {
    val initial = {
      val timestamp = DateTime.now(DateTimeZone.UTC)
      status.copy(lifecycle = ProbeSynthetic, health = ProbeUnknown, lastUpdate = Some(timestamp), lastChange = Some(timestamp))
    }
    ConfigureEffect(initial, Vector.empty, children, Set.empty)
  }

  def processEvaluation(probe: AccessorOps, command: ProcessCheckEvaluation): Try[CommandEffect] = Failure(ApiException(BadRequest))

  def processChild(probe: AccessorOps, childRef: ProbeRef, childStatus: ProbeStatus): Option[EventEffect] = None

  def processExpiryTimeout(probe: AccessorOps): Option[EventEffect] = None

  def processAlertTimeout(probe: AccessorOps): Option[EventEffect] = None
}

class ContainerCheck extends CheckBehaviorExtension {
  type Settings = ContainerCheckSettings
  class ContainerProcessorFactory(val settings: ContainerCheckSettings) extends DependentProcessorFactory {
    def implement() = new ContainerProcessor(settings)
  }
  def configure(properties: Map[String,String]) = {
    new ContainerProcessorFactory(ContainerCheckSettings())
  }
}
