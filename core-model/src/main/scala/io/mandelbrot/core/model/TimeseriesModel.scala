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

object TimeseriesModel

final class ObservationSource(val scheme: String, val id: String) extends Ordered[ObservationSource] {
  def compare(that: ObservationSource): Int = {
    var result = scheme.compareToIgnoreCase(that.scheme)
    if (result == 0) {
      result = id.compareTo(that.id)
    }
    result
  }
  override def equals(other: Any): Boolean = other match {
    case other: ObservationSource => scheme.equals(other.scheme) && id.equals(other.id)
    case _ => false
  }
  override def toString = scheme + ":" + id
  override def hashCode() = toString.hashCode
}

/**
 *
 */
sealed trait EvaluationSource {
  def toObservationSource: ObservationSource
  def samplingRate: SamplingRate
}

/**
 * A MetricSource uniquely identifies a metric within a Agent.
 */
final class MetricSource(val probeId: ProbeId,
                         val metricName: String,
                         val dimension: Dimension,
                         val statistic: Statistic,
                         val samplingRate: SamplingRate) extends EvaluationSource {

  def toObservationSource = new ObservationSource("probe", toString)

  override def equals(other: Any): Boolean = other match {
    case other: MetricSource =>
      probeId.equals(other.probeId) &&
        metricName.equals(other.metricName) &&
        dimension.equals(other.dimension) &&
        statistic.equals(other.statistic) &&
        samplingRate.equals(other.samplingRate)
    case _ => false
  }

  override def toString = probeId.toString + ":" + metricName + ":" + dimension.name + "=" + dimension.value + ":" + statistic.name + ":" + samplingRate.name
  override def hashCode() = toString.hashCode
}

object MetricSource {

  def apply(probeId: ProbeId, metricName: String, dimension: Dimension, statistic: Statistic, samplingRate: SamplingRate): MetricSource = {
    new MetricSource(probeId, metricName, dimension, statistic, samplingRate)
  }

  val MetricSourceMatcher = "probe:([^:]+):([^:]+):([a-zA-Z][a-zA-Z0-9-_.]*)=([^:]+):(.+)([a-zA-Z][a-zA-Z0-9-_.]*):([a-zA-Z][a-zA-Z0-9-_.]*)".r

  def apply(string: String): MetricSource = string match {
    case MetricSourceMatcher(probeId, metricName, dimensionName, dimensionValue, statistic, samplingRate) =>
      new MetricSource(ProbeId(probeId), metricName, Dimension(dimensionName, dimensionValue),
        Statistic.fromString(statistic), SamplingRate.fromString(samplingRate))
  }

  def unapply(source: MetricSource): Option[(ProbeId, String, Dimension, Statistic, SamplingRate)] = {
    Some((source.probeId, source.metricName, source.dimension, source.statistic, source.samplingRate))
  }
}
