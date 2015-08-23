package io.mandelbrot.core.model

import io.mandelbrot.core.util.CircularBuffer

sealed trait MetricsModel

abstract class TimeseriesSource(val segments: Vector[String], val id: String) extends Ordered[TimeseriesSource] with MetricsModel {
  def compare(that: TimeseriesSource): Int = {
    segments.zipAll(that.segments, null, null).foreach {
      case (thisSegment, thatSegment) if thatSegment == null => return 1
      case (thisSegment, thatSegment) if thisSegment == null => return -1
      case (thisSegment, thatSegment) =>
        val comparison = thisSegment.compareTo(thatSegment)
        if (comparison != 0) return comparison
    }
    id.compareTo(that.id)
  }
}

/**
 * A MetricSource uniquely identifies a metric within a Agent.
 */
class MetricSource(val probeId: ProbeId, val metricName: String) extends TimeseriesSource(probeId.segments, metricName) {
  override def equals(other: Any): Boolean = other match {
    case other: MetricSource => probeId.equals(other.probeId) && metricName.equals(other.metricName)
    case _ => false
  }
  override def toString = probeId.toString + ":" + metricName
  override def hashCode() = toString.hashCode
}

object MetricSource {
  def apply(probeId: ProbeId, metricName: String): MetricSource = new MetricSource(probeId, metricName)
  def apply(string: String): MetricSource = {
    val index = string.indexOf(':')
    if (index == -1) throw new IllegalArgumentException()
    val (probeId,metricName) = string.splitAt(index)
    new MetricSource(ProbeId(probeId), metricName.tail)
  }
  def unapply(source: MetricSource): Option[(ProbeId, String)] = Some((source.probeId, source.metricName))
}

/**
 *
 */
class ConsolidationWindow(size: Int) extends CircularBuffer[Option[BigDecimal]](size) with MetricsModel

/**
 *
 */
sealed trait ConsolidationFunction extends MetricsModel {
  def name: String
  def apply(values: Vector[BigDecimal]): Option[BigDecimal]
}

case object ConsolidateLast extends ConsolidationFunction {
  val name = "last"
  def apply(values: Vector[BigDecimal]) = values.lastOption
}

case object ConsolidateFirst extends ConsolidationFunction {
  val name = "first"
  def apply(values: Vector[BigDecimal]) = values.headOption
}

case object ConsolidateMin extends ConsolidationFunction {
  val name = "min"
  def apply(values: Vector[BigDecimal]): Option[BigDecimal] = values.foldLeft[Option[BigDecimal]](None) {
    case (None, value) => Some(value)
    case (curr, value) => if (curr.get <= value) curr else Some(value)
  }
}

case object ConsolidateMax extends ConsolidationFunction {
  val name = "max"
  def apply(values: Vector[BigDecimal]): Option[BigDecimal] = values.foldLeft[Option[BigDecimal]](None) {
    case (None, value) => Some(value)
    case (curr, value) => if (curr.get >= value) curr else Some(value)
  }
}

case object ConsolidateMean extends ConsolidationFunction {
  val name = "mean"
  def apply(values: Vector[BigDecimal]): Option[BigDecimal] = {
    if (values.isEmpty) None else Some(values.sum / values.length)
  }
}