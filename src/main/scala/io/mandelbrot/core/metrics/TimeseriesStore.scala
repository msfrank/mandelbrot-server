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

  private val timeseries = new java.util.TreeMap[TimeseriesSource, TimeseriesWindow]

  /**
   *
   */
  def append(checkId: CheckId, checkStatus: CheckStatus): Unit = {
    timeseries.tailMap(new TimeseriesSource(checkId))
      .filter { case (source: TimeseriesSource, _) => source.checkId.equals(checkId) }
      .foreach {
        case (source: TimeseriesSource, window: TimeseriesWindow) => window.append(checkStatus)
      }
  }

  def window(source: TimeseriesSource): TimeseriesWindow = timeseries(source)

  def windowOption(source: TimeseriesSource): Option[TimeseriesWindow] = Option(timeseries.get(source))

  def apply(source: TimeseriesSource, index: Int): CheckStatus = timeseries(source)(index)

  def get(source: TimeseriesSource, index: Int): Option[CheckStatus] = Option(timeseries.get(source)).flatMap(_.get(index))

  def head(source: TimeseriesSource): CheckStatus = timeseries(source).head

  def headOption(source: TimeseriesSource): Option[CheckStatus] = Option(timeseries.get(source)).flatMap(_.headOption)

  def sources(): Set[TimeseriesSource] = timeseries.keySet().toSet

  def windows(): Map[TimeseriesSource,TimeseriesWindow] = timeseries.toMap

  def resize(evaluation: TimeseriesEvaluation): Unit = {
    evaluation.sizing.foreach { case (source,size) =>
      timeseries.get(source) match {
        case null => timeseries(source) = new TimeseriesWindow(size)
        case window: TimeseriesWindow if size == window.size => // do nothing
        case window: TimeseriesWindow => timeseries(source).resize(size)
      }
    }
  }
}
