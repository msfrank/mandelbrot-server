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

package io.mandelbrot.core.metrics

import org.HdrHistogram.DoubleHistogram
import scala.collection.JavaConversions._

import io.mandelbrot.core.model._

object HistogramOps {

  def mergeHistograms(head: DoubleHistogram, tail: Vector[DoubleHistogram]): DoubleHistogram = if (tail.nonEmpty) {
    tail.foldLeft(head.copy()) {
      case (next, acc) =>
        acc.add(next)
        acc
    }
  } else head

  def histogram2statisticValues(histogram: DoubleHistogram, statistics: Set[Statistic]): Vector[StatisticValue] = {
    statistics.map {
      case statistic @ MetricMinimum =>
        StatisticValue(statistic, histogram.getMinValue)
      case statistic @ MetricNonzeroMinimum =>
        StatisticValue(statistic, histogram.getMinNonZeroValue)
      case statistic @ MetricMaximum =>
        StatisticValue(statistic, histogram.getMaxValue)
      case statistic @ MetricMean =>
        StatisticValue(statistic, histogram.getMean)
      case statistic @ MetricStandardDeviation =>
        StatisticValue(statistic, histogram.getStdDeviation)
      case statistic @ MetricSampleCount =>
        StatisticValue(statistic, histogram.getTotalCount)
      case statistic @ MetricSum =>
        val sum: Double = histogram.recordedValues()
          .lastOption.map(_.getTotalValueToThisValue)
          .getOrElse(0.0)
        StatisticValue(statistic, sum)
      case statistic @ Metric25thPercentile =>
        StatisticValue(statistic, histogram.getValueAtPercentile(25.0))
      case statistic @ Metric50thPercentile =>
        StatisticValue(statistic, histogram.getValueAtPercentile(50.0))
      case statistic @ Metric75thPercentile =>
        StatisticValue(statistic, histogram.getValueAtPercentile(75.0))
      case statistic @ Metric90thPercentile =>
        StatisticValue(statistic, histogram.getValueAtPercentile(90.0))
      case statistic @ Metric95thPercentile =>
        StatisticValue(statistic, histogram.getValueAtPercentile(95.0))
      case statistic @ Metric99thPercentile =>
        StatisticValue(statistic, histogram.getValueAtPercentile(99.0))
    }.toVector
  }
}
