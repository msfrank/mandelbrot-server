package io.mandelbrot.core.model

import io.mandelbrot.core.util.CircularBuffer

sealed trait MetricsModel

/**
 *
 */
class ConsolidationWindow(size: Int) extends CircularBuffer[Option[BigDecimal]](size) with MetricsModel {

}

sealed trait ConsolidationFunction extends MetricsModel {
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