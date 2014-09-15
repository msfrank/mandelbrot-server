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
import scala.util.parsing.combinator.JavaTokenParsers
import scala.collection.mutable

case class MetricSource(probePath: Vector[String], metricName: String)

/**
 *
 */
class MetricValue(anyVal: AnyVal) extends Ordered[MetricValue] {

  assert(anyVal.isInstanceOf[Long] || anyVal.isInstanceOf[Double])

  def compare(that: MetricValue): Int = anyVal match {
    case value: Long => value.compareTo(that.toLong)
    case value: Double => value.compareTo(that.toDouble)
    case other => throw new IllegalArgumentException()
  }

  override def equals(that: Any): Boolean = that match {
    case value: MetricValue => value.compare(this) == 0
    case value: Long => value.equals(toLong)
    case value: Double => value.equals(toDouble)
    case other => throw new IllegalArgumentException()
  }

  def toLong: Long = anyVal match {
    case value: Long => value
    case value: Double => value.toLong
  }

  def toDouble: Double = anyVal match {
    case value: Long => value.toDouble
    case value: Double => value
  }

  override def toString = anyVal.toString
}

object MetricValue {
  def apply(value: Long) = new MetricValue(value)
  def apply(value: Double) = new MetricValue(value)
  def apply(value: Int) = new MetricValue(value.toLong)
  def apply(value: Float) = new MetricValue(value.toDouble)
}

/**
 *
 */
class MetricWindow(val size: Int) {
  if (size <= 0) throw new IllegalArgumentException()

  private var curr: Int = 0
  private var array = Array.fill[Option[MetricValue]](size)(None)

  def push(value: MetricValue): Unit = {
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

  def apply(index: Int): MetricValue = array(lookup(index)).get
  def get(index: Int): Option[MetricValue] = array(lookup(index))
  def head: MetricValue = apply(0)
  def headOption: Option[MetricValue] = get(0)
  def foldLeft[A](z: A)(op: (MetricValue, A) => A): A = {
    var out = z
    for (i <- 0.until(array.size)) {
      get(i) match {
        case None => return out
        case Some(v) => out = op(v, out)
      }
    }
    out
  }

  def resize(_size: Int): Unit = {
    if (_size <= 0) throw new IllegalArgumentException()
    val resized = Array.fill[Option[MetricValue]](_size)(None)
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

  def apply(source: MetricSource, index: Int): MetricValue = metrics(source)(index)
  def get(source: MetricSource, index: Int): Option[MetricValue] = metrics.get(source).flatMap(_.get(index))
  def head(source: MetricSource): MetricValue = metrics(source).head
  def headOption(source: MetricSource): Option[MetricValue] = metrics.get(source).flatMap(_.headOption)
  def push(source: MetricSource, value: MetricValue): Unit = metrics.get(source).map(_.push(value))

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

/**
 *
 */
sealed trait MetricsEvaluation {
  def evaluate(metrics: MetricsStore): Boolean
  def sizing: Map[MetricSource,Int]
}

/**
 *
 */
case class ValueEquals(source: MetricSource, rhs: MetricValue) extends MetricsEvaluation {
  def evaluate(metrics: MetricsStore): Boolean = metrics.headOption(source) match {
    case Some(lhs: MetricValue) => lhs == rhs
    case None => false
  }
  def sizing = Map(source -> 1)
}

/**
 *
 */
case class ValueNotEquals(source: MetricSource, rhs: MetricValue) extends MetricsEvaluation {
  def evaluate(metrics: MetricsStore): Boolean = metrics.headOption(source) match {
    case Some(lhs: MetricValue) => lhs != rhs
    case None => false
  }
  def sizing = Map(source -> 1)
}

/**
 *
 */
case class ValueGreaterThan(source: MetricSource, rhs: MetricValue) extends MetricsEvaluation {
  def evaluate(metrics: MetricsStore): Boolean = metrics.headOption(source) match {
    case Some(lhs: MetricValue) => lhs > rhs
    case None => false
  }
  def sizing = Map(source -> 1)
}

/**
 *
 */
case class ValueLessThan(source: MetricSource, rhs: MetricValue) extends MetricsEvaluation {
  def evaluate(metrics: MetricsStore): Boolean = metrics.headOption(source) match {
    case Some(lhs: MetricValue) => lhs < rhs
    case None => false
  }
  def sizing = Map(source -> 1)
}

/**
 *
 */
case class LogicalAnd(children: Vector[MetricsEvaluation]) extends MetricsEvaluation {
  def evaluate(metrics: MetricsStore): Boolean = {
    children.foreach { child => if (!child.evaluate(metrics)) return false }
    true
  }
  def sizing = children.foldLeft(Map.empty[MetricSource,Int]) { case (map,child) =>
    map ++ child.sizing.filter {
      case (source, sizing) => sizing > map.getOrElse(source, 0)
    }
  }
}

/**
 *
 */
case class LogicalOr(children: Vector[MetricsEvaluation]) extends MetricsEvaluation {
  def evaluate(metrics: MetricsStore): Boolean = {
    children.foreach { child => if (child.evaluate(metrics)) return true}
    false
  }
  def sizing = children.foldLeft(Map.empty[MetricSource,Int]) { case (map,child) =>
    map ++ child.sizing.filter {
      case (source, sizing) => sizing > map.getOrElse(source, 0)
    }
  }
}

/**
 *
 */
case class LogicalNot(child: MetricsEvaluation) extends MetricsEvaluation {
  def evaluate(metrics: MetricsStore): Boolean = !child.evaluate(metrics)
  def sizing = child.sizing
}

case object AlwaysTrue extends MetricsEvaluation {
  def evaluate(metrics: MetricsStore): Boolean = true
  def sizing = Map.empty
}

case object AlwaysFalse extends MetricsEvaluation {
  def evaluate(metrics: MetricsStore): Boolean = false
  def sizing = Map.empty
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

  def metricValue: Parser[MetricValue] = wholeNumber ^^ { case v => MetricValue(v.toLong) }

  def equalsScalar: Parser[MetricsEvaluation] = _log(metricSource ~ literal("==") ~ metricValue)("equalsScalar") ^^ {
    case source ~ "==" ~ value => ValueEquals(source, value)
  }

  def notEqualsScalar: Parser[MetricsEvaluation] = _log(metricSource ~ literal("!=") ~ metricValue)("notEqualsScalar") ^^ {
    case source ~ "!=" ~ value => ValueNotEquals(source, value)
  }

  def lessThanScalar: Parser[MetricsEvaluation] = _log(metricSource ~ literal("<") ~ metricValue)("lessThanScalar") ^^ {
    case source ~ "<" ~ value => ValueLessThan(source, value)
  }

  def greaterThanScalar: Parser[MetricsEvaluation] = _log(metricSource ~ literal(">") ~ metricValue)("greaterThanScalar") ^^ {
    case source ~ ">" ~ value => ValueGreaterThan(source, value)
  }

  def scalarEvaluation: Parser[MetricsEvaluation] = _log(equalsScalar | notEqualsScalar | lessThanScalar | greaterThanScalar)("scalarEvaluation")

  /*
   * <Query>        ::= <OrOperator>
   * <OrOperator>   ::= <AndOperator> ('OR' <AndOperator>)*
   * <AndOperator>  ::= <NotOperator> ('AND' <NotOperator>)*
   * <NotOperator>  ::= ['NOT'] <NotOperator> | <Group>
   * <Group>        ::= '(' <OrOperator> ')' | <Expression>
   */

  def groupOperator: Parser[MetricsEvaluation] = _log((literal("(") ~> orOperator <~ literal(")")) | scalarEvaluation)("groupOperator") ^^ {
    case group: MetricsEvaluation => group
  }

  def notOperator: Parser[MetricsEvaluation] = _log(("not" ~ notOperator) | groupOperator)("notOperator") ^^ {
    case "not" ~ (not: MetricsEvaluation) => LogicalNot(not)
    case group: MetricsEvaluation => group
  }

  def andOperator: Parser[MetricsEvaluation] = _log(notOperator ~ rep("and" ~ notOperator))("andOperator") ^^ {
    case not1 ~ nots if nots.isEmpty =>
      not1
    case not1 ~ nots =>
      val children: Vector[MetricsEvaluation] = nots.map { case "and" ~ group => group }.toVector
      LogicalAnd(not1 +: children)
  }

  def orOperator: Parser[MetricsEvaluation] = _log(andOperator ~ rep("or" ~ andOperator))("orOperator") ^^ {
    case and1 ~ ands if ands.isEmpty =>
      and1
    case and1 ~ ands =>
      val children: Vector[MetricsEvaluation] = ands.map { case "or" ~ group => group }.toVector
      LogicalOr(and1 +: children)
  }

  /* the entry point */
  val ruleExpression: Parser[MetricsEvaluation] = _log(orOperator)("ruleExpression")

  /* */
  def whenClause: Parser[MetricsEvaluation] = _log(literal("when") ~ ruleExpression)("whenClause") ^^ {
    case "when" ~ expression => expression
  }

  /* */
  def unlessClause: Parser[MetricsEvaluation] = _log(literal("unless") ~ ruleExpression)("unlessClause") ^^ {
    case "unless" ~ expression => LogicalNot(expression)
  }

  def metricsEvaluation: Parser[MetricsEvaluation] = _log(whenClause | unlessClause)("metricsEvaluation")

  def parseMetricsEvaluation(input: String): MetricsEvaluation = parseAll(metricsEvaluation, input) match {
    case Success(evaluation: MetricsEvaluation, _) => evaluation
    case Success(other, _) => throw new Exception("unexpected parse result")
    case failure : NoSuccess => throw new Exception(failure.msg)
  }
}