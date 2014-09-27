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
import scala.concurrent.duration.FiniteDuration
import scala.util.parsing.combinator.JavaTokenParsers
import scala.collection.mutable
import scala.math.BigDecimal

case class MetricSource(probePath: Vector[String], metricName: String)

/**
 *
 */
class MetricWindow(val size: Int) {
  if (size <= 0) throw new IllegalArgumentException()

  private var curr: Int = 0
  private var array = Array.fill[Option[BigDecimal]](size)(None)

  def push(value: BigDecimal): Unit = {
    array(curr) = Some(value)
    curr = if (curr + 1 == array.size) 0 else curr + 1
  }

  private def lookup(index: Int): Int = {
    if (index < 0 || index >= array.size) throw new IndexOutOfBoundsException()
    if (curr - index - 1 < 0)
      array.size + (curr - index - 1)
    else
      curr - index - 1
  }

  def apply(index: Int): BigDecimal = array(lookup(index)).get
  def get(index: Int): Option[BigDecimal] = array(lookup(index))
  def head: BigDecimal = apply(0)
  def headOption: Option[BigDecimal] = get(0)
  def foldLeft[A](z: A)(op: (BigDecimal, A) => A): A = {
    var out = z
    for (i <- 0.until(array.size)) {
      get(i) match {
        case None => return out
        case Some(v) => out = op(v, out)
      }
    }
    out
  }

  def resize(newSize: Int): Unit = {
    if (newSize <= 0) throw new IllegalArgumentException()
    val resized = Array.fill[Option[BigDecimal]](newSize)(None)
    var next = curr
    var index = 0
    for (_ <- 0.until(array.size)) {
      resized(index) = array(next)
      index = if (index + 1 == resized.size) 0 else index + 1
      next = if (next + 1 == array.size) 0 else next + 1
    }
    curr = index
    array = resized
  }
}

/**
 *
 */
class MetricsStore(evaluation: MetricsEvaluation) {
  private val metrics = new mutable.HashMap[MetricSource, MetricWindow]
  resize(evaluation)

  def window(source: MetricSource): MetricWindow = metrics(source)
  def windowOption(source: MetricSource): Option[MetricWindow] = metrics.get(source)

  def apply(source: MetricSource, index: Int): BigDecimal = metrics(source)(index)
  def get(source: MetricSource, index: Int): Option[BigDecimal] = metrics.get(source).flatMap(_.get(index))
  def head(source: MetricSource): BigDecimal = metrics(source).head
  def headOption(source: MetricSource): Option[BigDecimal] = metrics.get(source).flatMap(_.headOption)
  def push(source: MetricSource, value: BigDecimal): Unit = metrics.get(source).map(_.push(value))

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
case class MetricsEvaluation(expression: EvaluationExpression) {
  def evaluate(metrics: MetricsStore): Option[Boolean] = expression.evaluate(metrics)
  def sizing: Map[MetricSource,Int] = expression.sources.map(_ -> 1).toMap
}

/**
 *
 */
class MetricsEvaluationParser extends JavaTokenParsers {

  val logger = LoggerFactory.getLogger(classOf[ProbeMatcherParser])

  /* shamelessly copied from Parsers.scala */
  def _log[T](p: => Parser[T])(name: String): Parser[T] = Parser { in =>
    logger.debug("trying " + name + " at "+ in)
    val r = p(in)
    logger.debug(name + " --> " + r)
    r
  }

  def bareSource: Parser[MetricSource] = regex("[a-zA-Z][a-zA-Z0-9_]*".r) ^^ { MetricSource(Vector.empty, _) }

  def qualifiedSource: Parser[MetricSource] = rep1(regex("""/[^#/]+""".r)) ~ literal("#") ~ bareSource ^^ {
    case (segments: List[String]) ~ "#" ~ MetricSource(_, name) => MetricSource(segments.toVector, name)
  }

  def metricSource = bareSource

  def metricValue: Parser[BigDecimal] = wholeNumber ^^ { case v => BigDecimal(v.toLong) }

  def equals: Parser[EvaluationExpression] = _log(metricSource ~ literal("==") ~ metricValue)("equals") ^^ {
    case source ~ "==" ~ value => EvaluateSource(source, HeadFunction(ValueEquals(value)))
  }

  def notEquals: Parser[EvaluationExpression] = _log(metricSource ~ literal("!=") ~ metricValue)("notEquals") ^^ {
    case source ~ "!=" ~ value => EvaluateSource(source, HeadFunction(ValueNotEquals(value)))
  }

  def lessThan: Parser[EvaluationExpression] = _log(metricSource ~ literal("<") ~ metricValue)("lessThan") ^^ {
    case source ~ "<" ~ value => EvaluateSource(source, HeadFunction(ValueLessThan(value)))
  }

  def lessThanEqual: Parser[EvaluationExpression] = _log(metricSource ~ literal("<=") ~ metricValue)("lessThanEqual") ^^ {
    case source ~ "<=" ~ value => EvaluateSource(source, HeadFunction(ValueLessEqualThan(value)))
  }

  def greaterThan: Parser[EvaluationExpression] = _log(metricSource ~ literal(">") ~ metricValue)("greaterThan") ^^ {
    case source ~ ">" ~ value => EvaluateSource(source, HeadFunction(ValueGreaterThan(value)))
  }

  def greaterThanEqual: Parser[EvaluationExpression] = _log(metricSource ~ literal(">=") ~ metricValue)("greaterThanEqual") ^^ {
    case source ~ ">=" ~ value => EvaluateSource(source, HeadFunction(ValueGreaterEqualThan(value)))
  }

  def scalarEvaluation: Parser[EvaluationExpression] = equals | notEquals | lessThanEqual | lessThan | greaterThanEqual | greaterThan

  /*
   * <Query>        ::= <OrOperator>
   * <OrOperator>   ::= <AndOperator> ('OR' <AndOperator>)*
   * <AndOperator>  ::= <NotOperator> ('AND' <NotOperator>)*
   * <NotOperator>  ::= ['NOT'] <NotOperator> | <Group>
   * <Group>        ::= '(' <OrOperator> ')' | <Expression>
   */

  def groupOperator: Parser[EvaluationExpression] = _log((literal("(") ~> orOperator <~ literal(")")) | scalarEvaluation)("groupOperator") ^^ {
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

  /* the entry point */
  val ruleExpression: Parser[EvaluationExpression] = _log(orOperator)("ruleExpression")

  /* */
  def whenClause: Parser[MetricsEvaluation] = _log(literal("when") ~ ruleExpression)("whenClause") ^^ {
    case "when" ~ expression => MetricsEvaluation(expression)
  }

  /* */
  def unlessClause: Parser[MetricsEvaluation] = _log(literal("unless") ~ ruleExpression)("unlessClause") ^^ {
    case "unless" ~ expression => MetricsEvaluation(LogicalNot(expression))
  }

  def metricsEvaluation: Parser[MetricsEvaluation] = _log(whenClause | unlessClause)("metricsEvaluation")

  def parseMetricsEvaluation(input: String): MetricsEvaluation = parseAll(metricsEvaluation, input) match {
    case Success(evaluation: MetricsEvaluation, _) => evaluation
    case Success(other, _) => throw new Exception("unexpected parse result")
    case failure : NoSuccess => throw new Exception(failure.msg)
  }
}