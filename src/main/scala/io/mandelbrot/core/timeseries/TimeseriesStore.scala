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

package io.mandelbrot.core.timeseries

import io.mandelbrot.core.model._

import scala.collection.JavaConversions._
import scala.concurrent.duration._

/**
 * A window of timeseries data.
 */
class TimeseriesWindow(initialSize: Int, initialInstant: Long, samplingRate: SamplingRate)
  extends SampleBuffer[ProbeMetrics](initialSize, initialInstant, samplingRate)

/**
 * 
 */
class TimeseriesStore(initialEvaluation: TimeseriesEvaluation, initialInstant: Option[Timestamp] = None) {

  private val _windows = new java.util.HashMap[EvaluationSource, TimeseriesWindow]

  // create the initial set of windows, and calculate the tick
  resize(initialEvaluation, initialInstant.getOrElse(Timestamp()))

  /**
   *
   */
  def put(source: MetricSource, metrics: ProbeMetrics): Unit = {
    _windows.get(source) match {
      case null =>  // do nothing
      case window: TimeseriesWindow => window.put(metrics.timestamp, metrics)
    }
  }

  /**
   *
   */
  def advance(timestamp: Timestamp): Unit = _windows.values().foreach(_.advance(timestamp))

  def window(source: ObservationSource): TimeseriesWindow = _windows.get(source)

  def window(source: EvaluationSource): TimeseriesWindow = window(source.toObservationSource)

  def windowOption(source: ObservationSource): Option[TimeseriesWindow] = Option(_windows.get(source))

  def windowOption(source: EvaluationSource): Option[TimeseriesWindow] = windowOption(source.toObservationSource)

  def sources(): Set[EvaluationSource] = _windows.keySet().toSet

  def windows(): Map[EvaluationSource,TimeseriesWindow] = _windows.toMap

  /**
   *
   */
  def resize(evaluation: TimeseriesEvaluation, instant: Timestamp): Unit = {
    // add or update windows
    evaluation.sizing.foreach { case (source: EvaluationSource, size: Int) =>
      _windows.get(source) match {
        case null =>
          _windows.put(source, new TimeseriesWindow(size, instant.toMillis, source.samplingRate))
        case window: TimeseriesWindow if size == window.size =>
          // do nothing
        case window: TimeseriesWindow =>
          window.resize(size, instant.toMillis)
      }
    }
    // remove any unused windows
    _windows.foreach {
      case (source: EvaluationSource, window: TimeseriesWindow) =>
        if (!evaluation.sources.contains(source))
          _windows.remove(source)
    }
  }

  /**
   *
   */
  def samplingRate: Option[SamplingRate] = {
    _windows.keys.foldLeft[Option[SamplingRate]](None) {
      case (None, source: EvaluationSource) => Some(source.samplingRate)
      case (acc @ Some(smallest), source: EvaluationSource) =>
        if (smallest.millis <= source.samplingRate.millis) acc else Some(source.samplingRate)
    }
  }
}

object TimeseriesStore {
  val tick1minute = 1.minute
  val tick5minute = 5.minutes
}
