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

import io.mandelbrot.core.util.CircularBuffer

/**
 *
 */
class ConsolidationWindow(size: Int) extends CircularBuffer[Option[BigDecimal]](size) {

}

sealed trait ConsolidationFunction {
  def name: String
  def apply(values: Vector[BigDecimal]): Option[BigDecimal]
}
case object ConsolidateLast extends ConsolidationFunction {
  val name = "last"
  def apply(values: Vector[BigDecimal]) = values.lastOption
}
case object ConsolidateFirst extends ConsolidationFunction {
  val name = "first"
  def apply(values: Vector[BigDecimal]) = values.headOption
}
case object ConsolidateMin extends ConsolidationFunction {
  val name = "min"
  def apply(values: Vector[BigDecimal]): Option[BigDecimal] = values.foldLeft[Option[BigDecimal]](None) {
    case (None, value) => Some(value)
    case (curr, value) => if (curr.get <= value) curr else Some(value)
  }
}
case object ConsolidateMax extends ConsolidationFunction {
  val name = "max"
  def apply(values: Vector[BigDecimal]): Option[BigDecimal] = values.foldLeft[Option[BigDecimal]](None) {
    case (None, value) => Some(value)
    case (curr, value) => if (curr.get >= value) curr else Some(value)
  }
}
case object ConsolidateMean extends ConsolidationFunction {
  val name = "mean"
  def apply(values: Vector[BigDecimal]): Option[BigDecimal] = {
    if (values.isEmpty) None else Some(values.foldLeft[BigDecimal](0)(_ + _) / values.length)
  }
}
