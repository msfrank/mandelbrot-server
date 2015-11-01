/**
 * Copyright 2015 Michael Frank <msfrank@syntaxjockey.com>
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

package io.mandelbrot.core.model

import org.joda.time.DateTime

object MetricsModel

final case class Dimension(name: String, value: String)

sealed trait Statistic {
  val name: String
}

case object MetricMinimum extends Statistic           { val name = "minimum" }
case object MetricMaximum extends Statistic           { val name = "maximum" }
case object MetricMean extends Statistic              { val name = "mean" }
case object MetricStandardDeviation extends Statistic { val name = "stddev" }
case object MetricNonzeroMinimum extends Statistic    { val name = "nzminimum" }
case object MetricSampleCount extends Statistic       { val name = "samples" }
case object MetricSum extends Statistic               { val name = "sum" }
case object Metric25thPercentile extends Statistic    { val name = "p25" }
case object Metric50thPercentile extends Statistic    { val name = "p50" }
case object Metric75thPercentile extends Statistic    { val name = "p75" }
case object Metric90thPercentile extends Statistic    { val name = "p90" }
case object Metric95thPercentile extends Statistic    { val name = "p95" }
case object Metric99thPercentile extends Statistic    { val name = "p99" }

object Statistic {
  def fromString(string: String): Statistic = string match {
    case MetricMinimum.name => MetricMinimum
    case MetricMaximum.name => MetricMaximum
    case MetricMean.name => MetricMean
    case MetricStandardDeviation.name => MetricStandardDeviation
    case MetricNonzeroMinimum.name => MetricNonzeroMinimum
    case MetricSampleCount.name => MetricSampleCount
    case MetricSum.name => MetricSum
    case Metric25thPercentile.name => Metric25thPercentile
    case Metric50thPercentile.name => Metric50thPercentile
    case Metric75thPercentile.name => Metric75thPercentile
    case Metric90thPercentile.name => Metric90thPercentile
    case Metric95thPercentile.name => Metric95thPercentile
    case Metric99thPercentile.name => Metric99thPercentile
    case _ => throw new IllegalArgumentException()
  }
}

final case class StatisticValue(statistic: Statistic, value: Double)

/* metrics for the given probe and dimension at the specified time */
final case class ProbeMetrics(probeId: ProbeId,
                        metricName: String,
                        dimension: Dimension,
                        timestamp: Timestamp,
                        statistics: Map[Statistic,Double])

/* a page of probe metric entries */
final case class ProbeMetricsPage(history: Vector[ProbeMetrics], last: Option[String], exhausted: Boolean)
