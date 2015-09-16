package io.mandelbrot.core.metrics

import io.mandelbrot.core.model._
import io.mandelbrot.core.util.CircularBuffer

import scala.collection.JavaConversions._

/**
 * A window of timeseries data.
 */
class TimeseriesWindow(size: Int) extends CircularBuffer[Observation](size)

/**
 * 
 */
class TimeseriesStore {

  private val observations = new java.util.HashMap[ObservationSource, TimeseriesWindow]
  private var tick: Option[SamplingRate] = None

  def append(source: ObservationSource, observation: Observation): Unit = {
    observations.get(source) match {
      case null =>  // do nothing
      case window: TimeseriesWindow => window.append(observation)
    }
  }

  def window(source: ObservationSource): TimeseriesWindow = observations.get(source)

  def window(source: EvaluationSource): TimeseriesWindow = window(source.toObservationSource)

  def windowOption(source: ObservationSource): Option[TimeseriesWindow] = Option(observations.get(source))

  def windowOption(source: EvaluationSource): Option[TimeseriesWindow] = windowOption(source.toObservationSource)

  def sources(): Set[ObservationSource] = observations.keySet().toSet

  def windows(): Map[ObservationSource,TimeseriesWindow] = observations.toMap

  private def calculateTick(): SamplingRate = PerMinute

  /**
   *
   */
  def resize(evaluation: TimeseriesEvaluation): Unit = {
    // add or update windows
    evaluation.sizing.foreach { case (source: ObservationSource, size: Int) =>
      observations.get(source) match {
        case null =>
          observations.put(source, new TimeseriesWindow(size))
        case window: TimeseriesWindow if size == window.size =>
          // do nothing
        case window: TimeseriesWindow =>
          window.resize(size)
      }
    }
    // remove any unused windows
    observations.foreach {
      case (source: ObservationSource, window: TimeseriesWindow) =>
        if (!evaluation.sources.contains(source))
          observations.remove(source)
    }
    // recalculate the sampling rate tick
  }
}
