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

import org.slf4j.LoggerFactory
import scala.collection.mutable
import scala.util.parsing.combinator.JavaTokenParsers
import scala.math.BigDecimal

import io.mandelbrot.core.model._
import io.mandelbrot.core.util.CircularBuffer

/**
 *
 */
class MetricsEvaluation(val expression: EvaluationExpression, input: String) {
  val sources: Set[MetricSource] = expression.sources
  val sizing: Map[MetricSource,Int] = expression.sources.map(_ -> 1).toMap
  def evaluate(metrics: MetricsStore): Option[Boolean] = expression.evaluate(metrics)
  override def toString = input
}

/* */
class MetricWindow(size: Int) extends CircularBuffer[BigDecimal](size)

/**
 *
 */
class MetricsStore(evaluation: MetricsEvaluation) {

  private val metrics = new mutable.HashMap[MetricSource, MetricWindow]
  resize(evaluation)

  def sources = metrics.keySet

  def window(source: MetricSource): MetricWindow = metrics(source)
  def windowOption(source: MetricSource): Option[MetricWindow] = metrics.get(source)

  def apply(source: MetricSource, index: Int): BigDecimal = metrics(source)(index)
  def get(source: MetricSource, index: Int): Option[BigDecimal] = metrics.get(source).flatMap(_.get(index))
  def head(source: MetricSource): BigDecimal = metrics(source).head
  def headOption(source: MetricSource): Option[BigDecimal] = metrics.get(source).flatMap(_.headOption)
  def append(source: MetricSource, value: BigDecimal): Unit = metrics.get(source).map(_.append(value))

  def resize(evaluation: MetricsEvaluation): Unit = {
    evaluation.sizing.foreach { case (source,size) =>
      metrics.get(source) match {
        case Some(window) if size == window.size => // do nothing
        case Some(window) => metrics(source).resize(size)
        case None => metrics(source) = new MetricWindow(size)
      }
    }
  }
}

sealed trait ValueComparison {
  def compare(value: BigDecimal): Boolean
}

case class ValueEquals(rhs: BigDecimal) extends ValueComparison {
  def compare(lhs: BigDecimal): Boolean = lhs == rhs
}

case class ValueNotEquals(rhs: BigDecimal) extends ValueComparison {
  def compare(lhs: BigDecimal): Boolean = lhs != rhs
}

case class ValueGreaterThan(rhs: BigDecimal) extends ValueComparison {
  def compare(lhs: BigDecimal): Boolean = lhs > rhs
}

case class ValueGreaterEqualThan(rhs: BigDecimal) extends ValueComparison {
  def compare(lhs: BigDecimal): Boolean = lhs >= rhs
}

case class ValueLessThan(rhs: BigDecimal) extends ValueComparison {
  def compare(lhs: BigDecimal): Boolean = lhs < rhs
}

case class ValueLessEqualThan(rhs: BigDecimal) extends ValueComparison {
  def compare(lhs: BigDecimal): Boolean = lhs <= rhs
}

/**
 *
 */
sealed trait WindowFunction {
  def apply(window: MetricWindow): Option[Boolean]
}

case class HeadFunction(comparison: ValueComparison) extends WindowFunction {
  def apply(window: MetricWindow): Option[Boolean] = window.headOption match {
    case Some(value) => if (comparison.compare(value)) Some(true) else Some(false)
    case None => None
  }
}

case class EachFunction(comparison: ValueComparison) extends WindowFunction {
  def apply(window: MetricWindow): Option[Boolean] = window.foldLeft(Some(true)) { case (value,result) =>
    if (!comparison.compare(value)) return Some(false)
    result
  }
}

case class MeanFunction(comparison: ValueComparison) extends WindowFunction {
  def apply(window: MetricWindow): Option[Boolean] = {
    val (sum, num) = window.foldLeft((BigDecimal(0), 0)) { case (value, (s, n)) => (s + value, n + 1)}
    Some(comparison.compare(sum / num))
  }
}

/**
 *
 */
sealed trait EvaluationExpression {
  def evaluate(metrics: MetricsStore): Option[Boolean]
  def sources: Set[MetricSource]
}

case class EvaluateSource(source: MetricSource, function: WindowFunction) extends EvaluationExpression {
  def evaluate(metrics: MetricsStore) = function.apply(metrics.window(source))
  val sources = Set(source)
}

case class LogicalAnd(children: Vector[EvaluationExpression]) extends EvaluationExpression {
  def evaluate(metrics: MetricsStore): Option[Boolean] = {
    children.foreach {
      child => child.evaluate(metrics) match {
        case None => return None
        case Some(false) => return Some(false)
        case Some(true) =>  // do nothing
      }
    }
    Some(true)
  }
  val sources = children.flatMap(_.sources).toSet
}

case class LogicalOr(children: Vector[EvaluationExpression]) extends EvaluationExpression {
  def evaluate(metrics: MetricsStore): Option[Boolean] = {
    children.foreach {
      child => child.evaluate(metrics) match {
        case None =>        // do nothing
        case Some(false) => // do nothing
        case Some(true) => return Some(true)
      }
    }
    Some(false)
  }
  val sources = children.flatMap(_.sources).toSet
}

case class LogicalNot(child: EvaluationExpression) extends EvaluationExpression {
  def evaluate(metrics: MetricsStore): Option[Boolean] = child.evaluate(metrics) match {
    case None => None
    case Some(result) => Some(!result)
  }
  val sources = child.sources
}

case object AlwaysTrue extends EvaluationExpression {
  def evaluate(metrics: MetricsStore): Option[Boolean] = Some(true)
  val sources = Set.empty[MetricSource]
}

case object AlwaysFalse extends EvaluationExpression {
  def evaluate(metrics: MetricsStore): Option[Boolean] = Some(false)
  val sources = Set.empty[MetricSource]
}

case object AlwaysUnknown extends EvaluationExpression {
  def evaluate(metrics: MetricsStore): Option[Boolean] = None
  val sources = Set.empty[MetricSource]
}

/**
 *
 */
class MetricsEvaluationParser extends JavaTokenParsers {

  val logger = LoggerFactory.getLogger(classOf[MetricsEvaluationParser])

  /* shamelessly copied from Parsers.scala */
  def _log[T](p: => Parser[T])(name: String): Parser[T] = Parser { in =>
    logger.debug("trying " + name + " at "+ in)
    val r = p(in)
    logger.debug(name + " --> " + r)
    r
  }

  def wholeNumberValue: Parser[BigDecimal] = wholeNumber ^^ { case v => BigDecimal(v.toLong) }
  def floatingPointNumberValue: Parser[BigDecimal] = floatingPointNumber ^^ { case v => BigDecimal(v.toDouble) }

  def metricValue: Parser[BigDecimal] = wholeNumberValue | floatingPointNumberValue

  def equals: Parser[ValueComparison] = _log(literal("==") ~ metricValue)("equals") ^^ {
    case "==" ~ value => ValueEquals(value)
  }

  def notEquals: Parser[ValueComparison] = _log(literal("!=") ~ metricValue)("notEquals") ^^ {
    case "!=" ~ value => ValueNotEquals(value)
  }

  def lessThan: Parser[ValueComparison] = _log(literal("<") ~ metricValue)("lessThan") ^^ {
    case "<" ~ value => ValueLessThan(value)
  }

  def lessThanEqual: Parser[ValueComparison] = _log(literal("<=") ~ metricValue)("lessThanEqual") ^^ {
    case "<=" ~ value => ValueLessEqualThan(value)
  }

  def greaterThan: Parser[ValueComparison] = _log(literal(">") ~ metricValue)("greaterThan") ^^ {
    case ">" ~ value => ValueGreaterThan(value)
  }

  def greaterThanEqual: Parser[ValueComparison] = _log(literal(">=") ~ metricValue)("greaterThanEqual") ^^ {
    case ">=" ~ value => ValueGreaterEqualThan(value)
  }

  def valueComparison: Parser[ValueComparison] = equals | notEquals | lessThanEqual | lessThan | greaterThanEqual | greaterThan

  def bareSource: Parser[MetricSource] = regex("[a-zA-Z][a-zA-Z0-9_]*".r) ^^ { MetricSource(Vector.empty, _) }

  def qualifiedSource: Parser[MetricSource] = rep1(regex("""/[^:/]+""".r)) ~ literal(":") ~ bareSource ^^ {
    case (segments: List[String]) ~ ":" ~ MetricSource(_, name) => MetricSource(segments.toVector.map(_.tail), name)
  }

  def metricSource: Parser[MetricSource] = bareSource | qualifiedSource

  def headFunction: Parser[EvaluationExpression] = metricSource ~ literal(".") ~ literal("head") ~ valueComparison ^^ {
    case source ~ "." ~ "head" ~ comparison => EvaluateSource(source, HeadFunction(comparison))
  }

  def eachFunction: Parser[EvaluationExpression] = metricSource ~ literal(".") ~ literal("each") ~ valueComparison ^^ {
    case source ~ "." ~ "each" ~ comparison => EvaluateSource(source, EachFunction(comparison))
  }

  def meanFunction: Parser[EvaluationExpression] = metricSource ~ literal(".") ~ literal("mean") ~ valueComparison ^^ {
    case source ~ "." ~ "mean" ~ comparison => EvaluateSource(source, MeanFunction(comparison))
  }

  def implicitFunction: Parser[EvaluationExpression] = metricSource ~ valueComparison ^^ {
    case source ~ comparison => EvaluateSource(source, HeadFunction(comparison))
  }

  def evaluationExpression: Parser[EvaluationExpression] = headFunction | eachFunction | meanFunction | implicitFunction

  /*
   * <Query>        ::= <OrOperator>
   * <OrOperator>   ::= <AndOperator> ('OR' <AndOperator>)*
   * <AndOperator>  ::= <NotOperator> ('AND' <NotOperator>)*
   * <NotOperator>  ::= ['NOT'] <NotOperator> | <Group>
   * <Group>        ::= '(' <OrOperator> ')' | <Expression>
   */

  def groupOperator: Parser[EvaluationExpression] = _log((literal("(") ~> orOperator <~ literal(")")) | evaluationExpression)("groupOperator") ^^ {
    case group: EvaluationExpression => group
  }

  def notOperator: Parser[EvaluationExpression] = _log(("not" ~ notOperator) | groupOperator)("notOperator") ^^ {
    case "not" ~ (not: EvaluationExpression) => LogicalNot(not)
    case group: EvaluationExpression => group
  }

  def andOperator: Parser[EvaluationExpression] = _log(notOperator ~ rep("and" ~ notOperator))("andOperator") ^^ {
    case not1 ~ nots if nots.isEmpty =>
      not1
    case not1 ~ nots =>
      val children: Vector[EvaluationExpression] = nots.map { case "and" ~ group => group }.toVector
      LogicalAnd(not1 +: children)
  }

  def orOperator: Parser[EvaluationExpression] = _log(andOperator ~ rep("or" ~ andOperator))("orOperator") ^^ {
    case and1 ~ ands if ands.isEmpty =>
      and1
    case and1 ~ ands =>
      val children: Vector[EvaluationExpression] = ands.map { case "or" ~ group => group }.toVector
      LogicalOr(and1 +: children)
  }

  val ruleExpression: Parser[EvaluationExpression] = _log(orOperator)("ruleExpression")

  /* */
  def whenClause: Parser[EvaluationExpression] = _log(literal("when") ~ ruleExpression)("whenClause") ^^ {
    case "when" ~ expression => expression
  }

  /* */
  def unlessClause: Parser[EvaluationExpression] = _log(literal("unless") ~ ruleExpression)("unlessClause") ^^ {
    case "unless" ~ expression => LogicalNot(expression)
  }

  /* the entry point */
  def metricsEvaluation: Parser[EvaluationExpression] = _log(whenClause | unlessClause)("metricsEvaluation")

  def parseMetricsEvaluation(input: String): MetricsEvaluation = parseAll(metricsEvaluation, input) match {
    case Success(expression: EvaluationExpression, _) => new MetricsEvaluation(expression, input)
    case Success(other, _) => throw new Exception("unexpected parse result")
    case failure : NoSuccess => throw new Exception("failed to parse MetricsEvaluation: " + failure.msg)
  }
}

object MetricsEvaluationParser {
  val parser = new MetricsEvaluationParser
  def parseMetricsEvaluation(input: String): MetricsEvaluation = parser.parseMetricsEvaluation(input)
}
