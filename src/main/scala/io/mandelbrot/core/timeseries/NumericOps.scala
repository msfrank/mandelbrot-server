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

/**
 * comparison functions for numeric (Double) values.
 */
sealed trait NumericValueComparison {
  def compare(value: Double): Boolean
}

case class NumericValueEquals(rhs: Double) extends NumericValueComparison {
  def compare(lhs: Double): Boolean = lhs == rhs
}

case class NumericValueNotEquals(rhs: Double) extends NumericValueComparison {
  def compare(lhs: Double): Boolean = lhs != rhs
}

case class NumericValueGreaterThan(rhs: Double) extends NumericValueComparison {
  def compare(lhs: Double): Boolean = lhs > rhs
}

case class NumericValueGreaterEqualThan(rhs: Double) extends NumericValueComparison {
  def compare(lhs: Double): Boolean = lhs >= rhs
}

case class NumericValueLessThan(rhs: Double) extends NumericValueComparison {
  def compare(lhs: Double): Boolean = lhs < rhs
}

case class NumericValueLessEqualThan(rhs: Double) extends NumericValueComparison {
  def compare(lhs: Double): Boolean = lhs <= rhs
}

/**
 * window functions for numeric (Double) values.
 */
sealed trait NumericWindowFunction {
  def apply(window: TimeseriesView[Double]): Option[Boolean]
}

case class HeadFunction(comparison: NumericValueComparison) extends NumericWindowFunction {
  def apply(window: TimeseriesView[Double]): Option[Boolean] = window.headOption match {
    case Some(value) => if (comparison.compare(value)) Some(true) else Some(false)
    case None => None
  }
}

case class EachFunction(comparison: NumericValueComparison) extends NumericWindowFunction {
  def apply(window: TimeseriesView[Double]): Option[Boolean] = window.foldLeft(Some(true)) { case (value,result) =>
    if (!comparison.compare(value)) return Some(false)
    result
  }
}

case class MinFunction(comparison: NumericValueComparison) extends NumericWindowFunction {
  def apply(window: TimeseriesView[Double]): Option[Boolean] = {
    val maybeMin = window.foldLeft[Option[Double]](None) {
      case (value,None) => Some(value)
      case (value,curr @ Some(_curr)) => if (_curr <= value) curr else Some(value)
    }
    maybeMin.map(comparison.compare)
  }
}

case class MaxFunction(comparison: NumericValueComparison) extends NumericWindowFunction {
  def apply(window: TimeseriesView[Double]): Option[Boolean] = {
    val maybeMax = window.foldLeft[Option[Double]](None) {
      case (value,None) => Some(value)
      case (value,curr @ Some(_curr)) => if (_curr >= value) curr else Some(value)
    }
    maybeMax.map(comparison.compare)
  }
}

case class MeanFunction(comparison: NumericValueComparison) extends NumericWindowFunction {
  def apply(window: TimeseriesView[Double]): Option[Boolean] = {
    val (sum, num) = window.foldLeft(0.toDouble, 0) { case (value, (s, n)) => (s + value, n + 1)}
    Some(comparison.compare(sum / num))
  }
}
