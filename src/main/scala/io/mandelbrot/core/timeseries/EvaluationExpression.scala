/**
 * Copyright 2014 Michael Frank <msfrank@syntaxjockey.com>
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

import io.mandelbrot.core.model.{MetricSource, ObservationSource}

/**
 * An expression which evaluates observations from a TimeseriesStore.
 */
sealed trait EvaluationExpression {
  def evaluate(timeseries: TimeseriesStore): Option[Boolean]
  def sources: Set[ObservationSource]
  def sizing: Set[(ObservationSource,Int)]
}

case class EvaluateMetric(source: MetricSource, function: NumericWindowFunction, options: WindowOptions) extends EvaluationExpression {
  def evaluate(timeseries: TimeseriesStore) = {
    val metricView = new TimeseriesMetricView(source, timeseries, options.windowSize)
    function.apply(metricView)
  }
  def sources = Set(source.toObservationSource)
  def sizing = Set((source.toObservationSource, options.windowSize))
}

/**
 * An evaluation expression which implements boolean logic (AND, OR, NOT).
 */
sealed trait LogicalGrouping extends EvaluationExpression {
  def children: Vector[EvaluationExpression]
}

case class LogicalAnd(children: Vector[EvaluationExpression]) extends LogicalGrouping {
  def evaluate(timeseries: TimeseriesStore): Option[Boolean] = {
    children.foreach {
      child => child.evaluate(timeseries) match {
        case None => return None
        case Some(false) => return Some(false)
        case Some(true) =>  // do nothing
      }
    }
    Some(true)
  }
  def sources = children.flatMap(_.sources).toSet
  def sizing = children.flatMap(_.sizing).toSet
}

case class LogicalOr(children: Vector[EvaluationExpression]) extends LogicalGrouping {
  def evaluate(timeseries: TimeseriesStore): Option[Boolean] = {
    children.foreach {
      child => child.evaluate(timeseries) match {
        case None =>        // do nothing
        case Some(false) => // do nothing
        case Some(true) => return Some(true)
      }
    }
    Some(false)
  }
  def sources = children.flatMap(_.sources).toSet
  def sizing = children.flatMap(_.sizing).toSet
}

case class LogicalNot(child: EvaluationExpression) extends LogicalGrouping {
  def evaluate(timeseries: TimeseriesStore): Option[Boolean] = child.evaluate(timeseries) match {
    case None => None
    case Some(result) => Some(!result)
  }
  lazy val children: Vector[EvaluationExpression] = Vector(child)
  def sources = child.sources
  def sizing = child.sizing
}

case object AlwaysTrue extends EvaluationExpression {
  def evaluate(timeseries: TimeseriesStore): Option[Boolean] = Some(true)
  def sources = Set.empty[ObservationSource]
  def sizing = Set.empty[(ObservationSource,Int)]
}

case object AlwaysFalse extends EvaluationExpression {
  def evaluate(timeseries: TimeseriesStore): Option[Boolean] = Some(false)
  def sources = Set.empty[ObservationSource]
  def sizing = Set.empty[(ObservationSource,Int)]
}

case object AlwaysUnknown extends EvaluationExpression {
  def evaluate(timeseries: TimeseriesStore): Option[Boolean] = None
  def sources = Set.empty[ObservationSource]
  def sizing = Set.empty[(ObservationSource,Int)]
}
