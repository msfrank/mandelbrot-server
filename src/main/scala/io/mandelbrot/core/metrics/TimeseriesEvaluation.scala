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
  val sources: Set[TimeseriesSource] = expression.sources
  val sizing: Map[TimeseriesSource,Int] = expression.sources.map(_ -> 1).toMap
  val observes: Set[ProbeId] = expression.observes
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

case class MeanFunction(comparison: NumericValueComparison) extends NumericWindowFunction {
  def apply(window: TimeseriesView[BigDecimal]): Option[Boolean] = {
    val (sum, num) = window.foldLeft((BigDecimal(0), 0)) { case (value, (s, n)) => (s + value, n + 1)}
    Some(comparison.compare(sum / num))
  }
}

/**
 *
 */
sealed trait EvaluationExpression {
  def evaluate(timeseries: TimeseriesStore): Option[Boolean]
  def sources: Set[TimeseriesSource]
  def observes: Set[ProbeId]
}

case class EvaluateMetric(source: MetricSource, function: NumericWindowFunction) extends EvaluationExpression {
  def evaluate(timeseries: TimeseriesStore) = {
    val metricView = new TimeseriesMetricView(timeseries.window(source), source.metricName)
    function.apply(metricView)
  }
  val sources = Set(source).toSet[TimeseriesSource]
  val observes = Set(source.probeId)
}

case class LogicalAnd(children: Vector[EvaluationExpression]) extends EvaluationExpression {
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
  val sources = children.flatMap(_.sources).toSet
  val observes = children.flatMap(_.observes).toSet
}

case class LogicalOr(children: Vector[EvaluationExpression]) extends EvaluationExpression {
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
  val sources = children.flatMap(_.sources).toSet
  val observes = children.flatMap(_.observes).toSet
}

case class LogicalNot(child: EvaluationExpression) extends EvaluationExpression {
  def evaluate(timeseries: TimeseriesStore): Option[Boolean] = child.evaluate(timeseries) match {
    case None => None
    case Some(result) => Some(!result)
  }
  val sources = child.sources
  val observes = child.observes
}

case object AlwaysTrue extends EvaluationExpression {
  def evaluate(timeseries: TimeseriesStore): Option[Boolean] = Some(true)
  val sources = Set.empty[TimeseriesSource]
  val observes = Set.empty[ProbeId]
}

case object AlwaysFalse extends EvaluationExpression {
  def evaluate(timeseries: TimeseriesStore): Option[Boolean] = Some(false)
  val sources = Set.empty[TimeseriesSource]
  val observes = Set.empty[ProbeId]
}

case object AlwaysUnknown extends EvaluationExpression {
  def evaluate(timeseries: TimeseriesStore): Option[Boolean] = None
  val sources = Set.empty[TimeseriesSource]
  val observes = Set.empty[ProbeId]
}

