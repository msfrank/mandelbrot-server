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

  def resize(evaluation: TimeseriesEvaluation): Unit = {
    evaluation.sizing.foreach { case (source: ObservationSource, size: Int) =>
      observations.get(source) match {
        case null => observations(source) = new TimeseriesWindow(size)
        case window: TimeseriesWindow if size == window.size => // do nothing
        case window: TimeseriesWindow => observations(source).resize(size)
      }
    }
  }
}
