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

package io.mandelbrot.core.timeseries

import io.mandelbrot.core.model._

/**
 *
 */
class TimeseriesEvaluation(val expression: EvaluationExpression, input: String) {

  val sources: Set[EvaluationSource] = expression.sources
  val sizing: Map[EvaluationSource,Int] = expression.sizing.foldLeft(Map.empty[EvaluationSource,Int]) {
    case (sizes, (source, size)) =>
      val max = sizes.getOrElse(source, 0).max(size)
      sizes + (source -> max)
  }
  val samplingRate = expression.samplingRate

  /**
   *
   */
  def evaluate(timeseries: TimeseriesStore): Option[Boolean] = expression.evaluate(timeseries)

  override def toString = input
}

sealed trait WindowUnit
case object WindowSamples extends WindowUnit
sealed trait WindowUnitOfTime extends WindowUnit { val millis: Long }
case object WindowMinutes extends WindowUnitOfTime { val millis = 60 * 1000L }
case object WindowHours extends WindowUnitOfTime { val millis = 60 * 60 * 1000L }
case object WindowDays extends WindowUnitOfTime { val millis = 24 * 60 * 60 * 1000L }

sealed trait WindowOptions {
  def windowSize: Int
  def windowUnit: WindowUnit
}
case class EvaluationOptions(windowSize: Int, windowUnit: WindowUnit) extends WindowOptions

case object LazyOptions extends WindowOptions {
  def windowSize: Int = throw new IllegalAccessException()
  def windowUnit: WindowUnit = throw new IllegalAccessException()
}
