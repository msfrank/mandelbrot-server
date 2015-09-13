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

package io.mandelbrot.core.metrics

import scala.math.BigDecimal

import io.mandelbrot.core.model._

/**
 *
 */
class TimeseriesEvaluation(val expression: EvaluationExpression, input: String) {
  val sources: Set[ObservationSource] = expression.sources
  val sizing: Map[ObservationSource,Int] = expression.sizing.foldLeft(Map.empty[ObservationSource,Int]) {
    case (sizes, (source,size)) =>
      val max = sizes.getOrElse(source, 0).max(size)
      sizes + (source -> max)
  }
  def evaluate(timeseries: TimeseriesStore): Option[Boolean] = expression.evaluate(timeseries)
  override def toString = input
}

sealed trait NumericValueComparison {
  def compare(value: BigDecimal): Boolean
}

case class NumericValueEquals(rhs: BigDecimal) extends NumericValueComparison {
  def compare(lhs: BigDecimal): Boolean = lhs == rhs
}

case class NumericValueNotEquals(rhs: BigDecimal) extends NumericValueComparison {
  def compare(lhs: BigDecimal): Boolean = lhs != rhs
}

case class NumericValueGreaterThan(rhs: BigDecimal) extends NumericValueComparison {
  def compare(lhs: BigDecimal): Boolean = lhs > rhs
}

case class NumericValueGreaterEqualThan(rhs: BigDecimal) extends NumericValueComparison {
  def compare(lhs: BigDecimal): Boolean = lhs >= rhs
}

case class NumericValueLessThan(rhs: BigDecimal) extends NumericValueComparison {
  def compare(lhs: BigDecimal): Boolean = lhs < rhs
}

case class NumericValueLessEqualThan(rhs: BigDecimal) extends NumericValueComparison {
  def compare(lhs: BigDecimal): Boolean = lhs <= rhs
}

/**
 *
 */
sealed trait NumericWindowFunction {
  def apply(window: TimeseriesView[BigDecimal]): Option[Boolean]
}

case class HeadFunction(comparison: NumericValueComparison) extends NumericWindowFunction {
  def apply(window: TimeseriesView[BigDecimal]): Option[Boolean] = window.headOption match {
    case Some(value) => if (comparison.compare(value)) Some(true) else Some(false)
    case None => None
  }
}

case class EachFunction(comparison: NumericValueComparison) extends NumericWindowFunction {
  def apply(window: TimeseriesView[BigDecimal]): Option[Boolean] = window.foldLeft(Some(true)) { case (value,result) =>
    if (!comparison.compare(value)) return Some(false)
    result
  }
}

case class MinFunction(comparison: NumericValueComparison) extends NumericWindowFunction {
  def apply(window: TimeseriesView[BigDecimal]): Option[Boolean] = {
    val maybeMin = window.foldLeft[Option[BigDecimal]](None) {
      case (value,None) => Some(value)
      case (value,curr @ Some(_curr)) => if (_curr <= value) curr else Some(value)
    }
    maybeMin.map(comparison.compare)
  }
}

case class MaxFunction(comparison: NumericValueComparison) extends NumericWindowFunction {
  def apply(window: TimeseriesView[BigDecimal]): Option[Boolean] = {
    val maybeMax = window.foldLeft[Option[BigDecimal]](None) {
      case (value,None) => Some(value)
      case (value,curr @ Some(_curr)) => if (_curr >= value) curr else Some(value)
    }
    maybeMax.map(comparison.compare)
  }
}

case class MeanFunction(comparison: NumericValueComparison) extends NumericWindowFunction {
  def apply(window: TimeseriesView[BigDecimal]): Option[Boolean] = {
    val (sum, num) = window.foldLeft((BigDecimal(0), 0)) { case (value, (s, n)) => (s + value, n + 1)}
    Some(comparison.compare(sum / num))
  }
}

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
 * A read-only projection of a single metric.
 */
class TimeseriesMetricView(source: MetricSource, timeseries: TimeseriesStore, size: Int) extends TimeseriesView[BigDecimal] {

  val window = timeseries.window(source)

  def apply(index: Int): BigDecimal = window(index) match {
    case observation: ScalarMapObservation => observation.metrics(source.metricName)
    case other => throw new IllegalStateException()
  }

  def get(index: Int): Option[BigDecimal] = window.get(index) match {
    case Some(observation: ScalarMapObservation) => observation.metrics.get(source.metricName)
    case other => None
  }

  def head: BigDecimal = window.head match {
    case observation: ScalarMapObservation => observation.metrics(source.metricName)
    case other => throw new IllegalStateException()
  }

  def headOption: Option[BigDecimal] = window.headOption match {
    case Some(observation: ScalarMapObservation) => observation.metrics.get(source.metricName)
    case other => None
  }

  def foldLeft[A](z: A)(op: (BigDecimal, A) => A): A = window.foldLeft(z) {
    case (observation: ScalarMapObservation, a) => op(observation.metrics(source.metricName), a)
    case (other, a) => throw new IllegalStateException()
  }
}

/**
 *
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

sealed trait WindowUnit
case object WindowSamples extends WindowUnit

sealed trait WindowOptions {
  def windowSize: Int
  def windowUnits: WindowUnit
}
case class EvaluationOptions(windowSize: Int, windowUnits: WindowUnit) extends WindowOptions

case object LazyOptions extends WindowOptions {
  def windowSize: Int = throw new IllegalAccessException()
  def windowUnits: WindowUnit = throw new IllegalAccessException()
}
