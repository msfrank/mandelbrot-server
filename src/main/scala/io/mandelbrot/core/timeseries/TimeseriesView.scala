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

import io.mandelbrot.core.model.{MetricSource, ScalarMapObservation}

/**
 * A typed, read-only projection of a timeseries window.
 */
sealed trait TimeseriesView[T] {
  def apply(index: Int): T
  def get(index: Int): Option[T]
  def head: T
  def headOption: Option[T]
  def foldLeft[A](z: A)(op: (T, A) => A): A
}

/**
 * A read-only projection of a single metric.
 */
class TimeseriesMetricView(source: MetricSource, timeseries: TimeseriesStore, size: Int) extends TimeseriesView[BigDecimal] {

  val window = timeseries.window(source)

  def apply(index: Int): BigDecimal = window(index) match {
    case observation: ScalarMapObservation => observation.metrics(source.metricName)
    case other => throw new IllegalStateException()
  }

  def get(index: Int): Option[BigDecimal] = window.get(index) match {
    case Some(observation: ScalarMapObservation) => observation.metrics.get(source.metricName)
    case other => None
  }

  def head: BigDecimal = window.head match {
    case observation: ScalarMapObservation => observation.metrics(source.metricName)
    case other => throw new IllegalStateException()
  }

  def headOption: Option[BigDecimal] = window.headOption match {
    case Some(observation: ScalarMapObservation) => observation.metrics.get(source.metricName)
    case other => None
  }

  def foldLeft[A](z: A)(op: (BigDecimal, A) => A): A = window.foldLeft(z) {
    case (observation: ScalarMapObservation, a) => op(observation.metrics(source.metricName), a)
    case (other, a) => throw new IllegalStateException()
  }
}
