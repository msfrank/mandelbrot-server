package io.mandelbrot.core.timeseries

import io.mandelbrot.core.model._
import io.mandelbrot.core.util.CircularBuffer

import scala.collection.JavaConversions._
import scala.concurrent.duration._

/**
 *
 */
case class TickObservation(tick: Tick, observation: Observation)

/**
 * A window of timeseries data.
 */
class TimeseriesWindow(size: Int) extends CircularBuffer[ProbeMetrics](size)

/**
 * 
 */
class TimeseriesStore(initial: TimeseriesEvaluation) {

  private val _windows = new java.util.HashMap[ObservationSource, TimeseriesWindow]
  private var _tick: FiniteDuration = null

  // create the initial set of windows, and calculate the tick
  resize(initial)

  /**
   *
   */
  def append(source: ObservationSource, metrics: ProbeMetrics): Unit = {
    _windows.get(source) match {
      case null =>  // do nothing
      case window: TimeseriesWindow => window.append(metrics)
    }
  }

  def window(source: ObservationSource): TimeseriesWindow = _windows.get(source)

  def window(source: EvaluationSource): TimeseriesWindow = window(source.toObservationSource)

  def windowOption(source: ObservationSource): Option[TimeseriesWindow] = Option(_windows.get(source))

  def windowOption(source: EvaluationSource): Option[TimeseriesWindow] = windowOption(source.toObservationSource)

  def sources(): Set[ObservationSource] = _windows.keySet().toSet

  def windows(): Map[ObservationSource,TimeseriesWindow] = _windows.toMap

  def tick(): FiniteDuration = _tick

  /**
   *
   */
  def resize(evaluation: TimeseriesEvaluation): Unit = {
    // add or update windows
    evaluation.sizing.foreach { case (source: ObservationSource, size: Int) =>
      _windows.get(source) match {
        case null =>
          _windows.put(source, new TimeseriesWindow(size))
        case window: TimeseriesWindow if size == window.size =>
          // do nothing
        case window: TimeseriesWindow =>
          window.resize(size)
      }
    }
    // remove any unused windows
    _windows.foreach {
      case (source: ObservationSource, window: TimeseriesWindow) =>
        if (!evaluation.sources.contains(source))
          _windows.remove(source)
    }
    // recalculate the sampling rate tick
    // FIXME: do the actual calculation based on window configuration
    _tick = TimeseriesStore.tick1minute
  }
}

object TimeseriesStore {
  val tick1minute = 1.minute
  val tick5minute = 5.minutes
}
