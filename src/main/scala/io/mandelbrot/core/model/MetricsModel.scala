package io.mandelbrot.core.model

import io.mandelbrot.core.util.CircularBuffer

sealed trait MetricsModel

sealed class TimeseriesSource(val checkId: CheckId) extends MetricsModel {
  override def hashCode() = toString.hashCode
  override def equals(other: Any): Boolean = other match {
    case other: TimeseriesSource => toString.equals(other.toString)
    case _ => false
  }
}

/**
 * A MetricSource uniquely identifies a metric within a Agent.
 */
class MetricSource(override val checkId: CheckId, val metricName: String) extends TimeseriesSource(checkId) with Ordered[MetricSource] {
  def compare(that: MetricSource): Int = toString.compareTo(that.toString)
  override def toString = checkId.toString + ":" + metricName
}

object MetricSource {
  def apply(checkId: CheckId, metricName: String): MetricSource = new MetricSource(checkId, metricName)

  def apply(string: String): MetricSource = {
    val index = string.indexOf(':')
    if (index == -1) throw new IllegalArgumentException()
    val (checkId,metricName) = string.splitAt(index)
    new MetricSource(CheckId(checkId), metricName.tail)
  }

  def unapply(source: MetricSource): Option[(CheckId, String)] = Some((source.checkId, source.metricName))
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