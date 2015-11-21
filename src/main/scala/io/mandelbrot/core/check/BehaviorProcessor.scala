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
import scala.concurrent.duration.FiniteDuration
import scala.util.{Success, Failure, Try}
import java.util.UUID

import io.mandelbrot.core.model._
import io.mandelbrot.core.{BadRequest, Conflict, ResourceNotFound, ApiException}

/**
 *
 */
trait BehaviorProcessor {

  /**
   * return the initializers needed to rebuild local state.
   */
  def initialize(checkRef: CheckRef, generation: Long): InitializeEffect

  /**
   *
   */
  def configure(checkRef: CheckRef,
                generation: Long,
                status: Option[CheckStatus],
                observations: Map[ProbeId,Vector[ProbeObservation]],
                children: Set[CheckRef]): ConfigureEffect

  /**
   *
   */
  def processMetric(check: AccessorOps, metric: ProbeMetrics): Option[EventEffect]

  /**
   *
   */
  def processStatus(check: AccessorOps, child: CheckRef, status: CheckStatus): Option[EventEffect]

  /**
   *
   */
  def processTick(check: AccessorOps): Option[EventEffect]

}

sealed trait CheckEffect
case class InitializeEffect(initializers: Map[ObservationSource,CheckInitializer]) extends CheckEffect
case class ConfigureEffect(status: CheckStatus,
                           notifications: Vector[CheckNotification],
                           children: Set[CheckRef],
                           tick: FiniteDuration) extends CheckEffect
case class CommandEffect(result: CheckResult,
                         status: CheckStatus,
                         notifications: Vector[CheckNotification]) extends CheckEffect

case class EventEffect(status: CheckStatus,
                       notifications: Vector[CheckNotification]) extends CheckEffect

