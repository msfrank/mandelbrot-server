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

package io.mandelbrot.core.system

import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.parsing.combinator.RegexParsers

sealed trait MetricSource
case class CounterSource(name: String) extends MetricSource
case class GaugeSource(name: String) extends MetricSource
case class MetricValue(v: Long)

/**
 *
 */
sealed trait MetricsEvaluation {
  def evaluate(metrics: mutable.HashMap[MetricSource,MetricValue]): Boolean
}

/**
 *
 */
case class ValueEquals(lhs: MetricSource, rhs: MetricValue) extends MetricsEvaluation {
  def evaluate(metrics: mutable.HashMap[MetricSource,MetricValue]): Boolean = metrics.get(lhs) match {
    case Some(MetricValue(v)) => v == rhs.v
    case None => false
  }
}

/**
 *
 */
case class ValueNotEquals(lhs: MetricSource, rhs: MetricValue) extends MetricsEvaluation {
  def evaluate(metrics: mutable.HashMap[MetricSource,MetricValue]): Boolean = metrics.get(lhs) match {
    case Some(MetricValue(v)) => v != rhs.v
    case None => false
  }
}

/**
 *
 */
case class ValueGreaterThan(lhs: MetricSource, rhs: MetricValue) extends MetricsEvaluation {
  def evaluate(metrics: mutable.HashMap[MetricSource,MetricValue]): Boolean = metrics.get(lhs) match {
    case Some(MetricValue(v)) => v > rhs.v
    case None => false
  }
}

/**
 *
 */
case class ValueLessThan(lhs: MetricSource, rhs: MetricValue) extends MetricsEvaluation {
  def evaluate(metrics: mutable.HashMap[MetricSource,MetricValue]): Boolean = metrics.get(lhs) match {
    case Some(MetricValue(v)) => v < rhs.v
    case None => false
  }
}

/**
 *
 */
case class LogicalAnd(children: Vector[MetricsEvaluation]) extends MetricsEvaluation {
  def evaluate(metrics: mutable.HashMap[MetricSource,MetricValue]): Boolean = {
    children.foreach { child => if (!child.evaluate(metrics)) return false }
    true
  }
}

/**
 *
 */
case class LogicalOr(children: Vector[MetricsEvaluation]) extends MetricsEvaluation {
  def evaluate(metrics: mutable.HashMap[MetricSource, MetricValue]): Boolean = {
    children.foreach { child => if (child.evaluate(metrics)) return true}
    false
  }
}

/**
 *
 */
case class LogicalNot(child: MetricsEvaluation) extends MetricsEvaluation {
  def evaluate(metrics: mutable.HashMap[MetricSource, MetricValue]): Boolean = !child.evaluate(metrics)
}

case object AlwaysTrue extends MetricsEvaluation {
  def evaluate(metrics: mutable.HashMap[MetricSource, MetricValue]): Boolean = true
}

case object AlwaysFalse extends MetricsEvaluation {
  def evaluate(metrics: mutable.HashMap[MetricSource, MetricValue]): Boolean = false
}

/**
 *
 */
class MetricsEvaluationParser extends RegexParsers {

  val logger = LoggerFactory.getLogger(classOf[ProbeMatcherParser])

  /* shamelessly copied from Parsers.scala */
  def _log[T](p: => Parser[T])(name: String): Parser[T] = Parser { in =>
    logger.debug("trying " + name + " at "+ in)
    val r = p(in)
    logger.debug(name + " --> " + r)
    r
  }

  def metricsEvaluation: Parser[MetricsEvaluation] = regex(".*".r) ^^ { case any => AlwaysTrue }

  def parseMetricsEvaluation(input: String): MetricsEvaluation = parseAll(metricsEvaluation, input) match {
    case Success(evaluation: MetricsEvaluation, _) => evaluation
    case Success(other, _) => throw new Exception("unexpected parse result")
    case failure : NoSuccess => throw new Exception(failure.msg)
  }
}