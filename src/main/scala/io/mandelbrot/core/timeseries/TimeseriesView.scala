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

import io.mandelbrot.core.model.{ProbeMetrics, MetricSource}

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
class TimeseriesMetricView(source: MetricSource, timeseries: TimeseriesStore, size: Int) extends TimeseriesView[Double] {

  val window = timeseries.window(source)

  def apply(index: Int): Double = window(index).statistics(source.statistic)

  def get(index: Int): Option[Double] = window.get(index).map(_.statistics(source.statistic))

  def head: Double = window.head.statistics(source.statistic)

  def headOption: Option[Double] = window.headOption.map(_.statistics(source.statistic))

  def foldLeft[A](z: A)(op: (Double, A) => A): A = window.foldLeft(z) {
    case (metrics: ProbeMetrics, a) => op(metrics.statistics(source.statistic), a)
  }
}
