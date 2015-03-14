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

import scala.collection.mutable

import io.mandelbrot.core.model._

/**
 *
 */
sealed trait AggregateEvaluation {
  def evaluate(children: mutable.HashMap[ProbeRef,Option[ProbeStatus]]): ProbeHealth
}

/**
 * evaluate the status of each child probe, and return the worst one (where the order
 * of severity is defined as Unknown, Failed, Degraded, Healthy).
 */
case object EvaluateWorst extends AggregateEvaluation {
  def evaluate(children: mutable.HashMap[ProbeRef,Option[ProbeStatus]]): ProbeHealth = {
    children.values.foldLeft[ProbeHealth](ProbeHealthy) {
      case (ProbeUnknown, _) => ProbeUnknown
      case (curr, None) => ProbeUnknown
      case (curr, Some(next)) =>
        next.health match {
          case ProbeFailed if curr != ProbeUnknown => next.health
          case ProbeDegraded if curr != ProbeUnknown && curr != ProbeFailed => next.health
          case ProbeHealthy if curr != ProbeUnknown && curr != ProbeFailed && curr != ProbeDegraded => next.health
          case _ => curr
        }
    }
  }
}