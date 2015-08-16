package io.mandelbrot.core.metrics

import io.mandelbrot.core.model._
import io.mandelbrot.core.util.CircularBuffer

import scala.collection.JavaConversions._

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
 * A window of timeseries data.
 */
class TimeseriesWindow(size: Int) extends CircularBuffer[CheckStatus](size) with TimeseriesView[CheckStatus]

/**
 * A read-only projection of a single metric.
 */
class TimeseriesMetricView(window: TimeseriesWindow, metricName: String) extends TimeseriesView[BigDecimal] {

  override def apply(index: Int): BigDecimal = window(index).metrics(metricName)

  override def get(index: Int): Option[BigDecimal] = window.get(index).flatMap(_.metrics.get(metricName))

  override def headOption: Option[BigDecimal] = window.headOption.flatMap(_.metrics.get(metricName))

  override def head: BigDecimal = window.head.metrics(metricName)

  override def foldLeft[A](z: A)(op: (BigDecimal, A) => A): A = window.foldLeft(z) {
    case (status: CheckStatus, a) => op(status.metrics(metricName), a)
  }
}

/**
 * 
 */
class TimeseriesStore {

  private val probes = new java.util.TreeMap[MetricSource, TimeseriesWindow]
  private val checks = new java.util.TreeMap[MetricSource, TimeseriesWindow]

  /**
   *
   */
  def append(checkId: CheckId, checkStatus: CheckStatus): Unit = {
    probes.tailMap(new MetricSource(checkId, ""))
      .filter { case (source: MetricSource, _) => source.checkId.equals(checkId) }
      .foreach {
        case (source: MetricSource, window: TimeseriesWindow) => window.append(checkStatus)
      }
  }

  def window(source: MetricSource): TimeseriesWindow = probes(source)

  def windowOption(source: MetricSource): Option[TimeseriesWindow] = Option(probes.get(source))

  def apply(source: MetricSource, index: Int): CheckStatus = probes(source)(index)

  def get(source: MetricSource, index: Int): Option[CheckStatus] = Option(probes.get(source)).flatMap(_.get(index))

  def head(source: MetricSource): CheckStatus = probes(source).head

  def headOption(source: MetricSource): Option[CheckStatus] = Option(probes.get(source)).flatMap(_.headOption)

  def sources(): Set[MetricSource] = probes.keySet().toSet

  def windows(): Map[MetricSource,TimeseriesWindow] = probes.toMap

  def resize(evaluation: TimeseriesEvaluation): Unit = {
    evaluation.sizing.foreach { case (source: MetricSource, size: Int) =>
      probes.get(source) match {
        case null => probes(source) = new TimeseriesWindow(size)
        case window: TimeseriesWindow if size == window.size => // do nothing
        case window: TimeseriesWindow => probes(source).resize(size)
      }
    }
  }
}
