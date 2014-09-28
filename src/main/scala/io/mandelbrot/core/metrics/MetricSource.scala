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

class MetricSource(val probePath: Vector[String], val metricName: String) extends Ordered[MetricSource] {

  def compare(that: MetricSource): Int = toString.compareTo(that.toString)

  override def equals(other: Any): Boolean = other match {
    case other: MetricSource => probePath.equals(other.probePath) && metricName.equals(other.metricName)
    case _ => false
  }

  override def hashCode() = toString.hashCode

  override def toString = if (probePath.isEmpty) metricName else probePath.mkString("/", "/", ":") + metricName
}

object MetricSource {
  def apply(probePath: Vector[String], metricName: String): MetricSource = new MetricSource(probePath, metricName)

  def apply(string: String): MetricSource = {
    val index = string.indexOf(':')
    if (index == -1) new MetricSource(Vector.empty, string) else {
      if (string.head != '/')
        throw new IllegalArgumentException()
      val (path,name) = string.splitAt(index)
      new MetricSource(path.tail.split('/').toVector, name)
    }
  }

  def unapply(source: MetricSource): Option[(Vector[String], String)] = Some((source.probePath, source.metricName))
}

sealed trait SourceType
case object GaugeSource extends SourceType    { override def toString = "gauge" }
case object CounterSource extends SourceType  { override def toString = "counter" }
