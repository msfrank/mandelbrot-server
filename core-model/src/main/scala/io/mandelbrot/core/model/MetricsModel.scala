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

sealed trait MetricsModel

case class Dimension(name: String, value: String) extends MetricsModel

sealed trait Statistic

case object MetricMinimum extends Statistic with MetricsModel
case object MetricMaximum extends Statistic with MetricsModel
case object MetricMean extends Statistic with MetricsModel
case object MetricStandardDeviation extends Statistic with MetricsModel
case object MetricNonzeroMinimum extends Statistic with MetricsModel
case object MetricSampleCount extends Statistic with MetricsModel
case object MetricSum extends Statistic with MetricsModel
case object Metric25thPercentile extends Statistic with MetricsModel
case object Metric50thPercentile extends Statistic with MetricsModel
case object Metric75thPercentile extends Statistic with MetricsModel
case object Metric90thPercentile extends Statistic with MetricsModel
case object Metric95thPercentile extends Statistic with MetricsModel
case object Metric99thPercentile extends Statistic with MetricsModel

case class StatisticValue(statistic: Statistic, value: Double) extends MetricsModel

/* metrics for the given probe and dimension at the specified time */
case class ProbeMetrics(probeId: ProbeId,
                        metricName: String,
                        dimension: Dimension,
                        timestamp: Timestamp,
                        statistics: Vector[StatisticValue]) extends MetricsModel

/* a page of probe metric entries */
case class ProbeMetricsPage(history: Vector[ProbeMetrics], last: Option[String], exhausted: Boolean) extends MetricsModel
