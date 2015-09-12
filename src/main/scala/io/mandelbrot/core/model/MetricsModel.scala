package io.mandelbrot.core.model

import io.mandelbrot.core.util.CircularBuffer

sealed trait MetricsModel

class ObservationSource(val scheme: String, val id: String) extends Ordered[ObservationSource] with MetricsModel {
  def compare(that: ObservationSource): Int = {
    var result = scheme.compareToIgnoreCase(that.scheme)
    if (result == 0) {
      result = id.compareTo(that.id)
    }
    result
  }
  override def toString = scheme + ":" + id
}

object ObservationSource {
  def apply(probeId: ProbeId) = new ObservationSource("probe", probeId.toString)
  def apply(probeRef: ProbeRef) = new ObservationSource("probe", probeRef.probeId.toString)
}

/**
 *
 */
sealed trait EvaluationSource {
  def toObservationSource: ObservationSource
}

/**
 * A MetricSource uniquely identifies a metric within a Agent.
 */
class MetricSource(val probeId: ProbeId, val metricName: String) extends EvaluationSource with MetricsModel {
  def toObservationSource = ObservationSource(probeId)
  override def equals(other: Any): Boolean = other match {
    case other: MetricSource => probeId.equals(other.probeId) && metricName.equals(other.metricName)
    case _ => false
  }
  override def toString = probeId.toString + ":" + metricName
  override def hashCode() = toString.hashCode
}

object MetricSource {
  def apply(probeId: ProbeId, metricName: String): MetricSource = new MetricSource(probeId, metricName)
  val MetricSourceMatcher = "probe:([^:]+):(.+)".r
  def apply(string: String): MetricSource = string match {
    case MetricSourceMatcher(probeId, metricName) => new MetricSource(ProbeId(probeId), metricName)
  }
  def unapply(source: MetricSource): Option[(ProbeId, String)] = Some((source.probeId, source.metricName))
}
