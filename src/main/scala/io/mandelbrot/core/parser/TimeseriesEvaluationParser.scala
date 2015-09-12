package io.mandelbrot.core.parser

import org.slf4j.LoggerFactory
import scala.util.parsing.combinator.JavaTokenParsers

import io.mandelbrot.core.model._
import io.mandelbrot.core.metrics._

/**
 *
 */
class TimeseriesEvaluationParser extends JavaTokenParsers {

  val logger = LoggerFactory.getLogger(classOf[TimeseriesEvaluationParser])

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

  def equals: Parser[NumericValueComparison] = _log(literal("==") ~ metricValue)("equals") ^^ {
    case "==" ~ value => NumericValueEquals(value)
  }

  def notEquals: Parser[NumericValueComparison] = _log(literal("!=") ~ metricValue)("notEquals") ^^ {
    case "!=" ~ value => NumericValueNotEquals(value)
  }

  def lessThan: Parser[NumericValueComparison] = _log(literal("<") ~ metricValue)("lessThan") ^^ {
    case "<" ~ value => NumericValueLessThan(value)
  }

  def lessThanEqual: Parser[NumericValueComparison] = _log(literal("<=") ~ metricValue)("lessThanEqual") ^^ {
    case "<=" ~ value => NumericValueLessEqualThan(value)
  }

  def greaterThan: Parser[NumericValueComparison] = _log(literal(">") ~ metricValue)("greaterThan") ^^ {
    case ">" ~ value => NumericValueGreaterThan(value)
  }

  def greaterThanEqual: Parser[NumericValueComparison] = _log(literal(">=") ~ metricValue)("greaterThanEqual") ^^ {
    case ">=" ~ value => NumericValueGreaterEqualThan(value)
  }

  def valueComparison: Parser[NumericValueComparison] = equals | notEquals | lessThanEqual | lessThan | greaterThanEqual | greaterThan

  def checkId: Parser[CheckId] = rep1sep(regex("""[^.:]+""".r), literal(".")) ^^ {
    case segments: List[String] => new CheckId(segments.toVector)
  }

  def probeId: Parser[ProbeId] = rep1sep(regex("""[^.:]+""".r), literal(".")) ^^ {
    case segments: List[String] => new ProbeId(segments.toVector)
  }

  def metricName: Parser[String] = regex("[a-zA-Z][a-zA-Z0-9_]*".r)

  def metricSource: Parser[MetricSource] = literal("probe") ~ literal(":") ~ probeId ~ literal(":") ~ metricName ^^ {
    case "probe" ~ ":" ~ (probeId: ProbeId) ~ ":" ~ (metricName: String) => MetricSource(probeId, metricName)
  }

  /*
   * <MINFunction>  ::= 'MIN' '(' <MetricSource> ')' <ValueComparison>
   * <MAXFunction>  ::= 'MAX' '(' <MetricSource> ')' <ValueComparison>
   * <AVGFunction>  ::= 'AVG' '(' <MetricSource> ')' <ValueComparison>
   */
  def minFunction: Parser[EvaluationExpression] = literal("MIN") ~ literal("(") ~ metricSource ~ literal(")") ~ valueComparison ^^ {
    case "MIN" ~ "(" ~ source ~ ")" ~ comparison => EvaluateMetric(source, MinFunction(comparison))
  }

  def maxFunction: Parser[EvaluationExpression] = literal("MAX") ~ literal("(") ~ metricSource ~ literal(")") ~ valueComparison ^^ {
    case "MAX" ~ "(" ~ source ~ ")" ~ comparison => EvaluateMetric(source, MaxFunction(comparison))
  }

  def meanFunction: Parser[EvaluationExpression] = literal("AVG") ~ literal("(") ~ metricSource ~ literal(")") ~ valueComparison ^^ {
    case "AVG" ~ "(" ~ source ~ ")" ~ comparison => EvaluateMetric(source, MeanFunction(comparison))
  }

  def implicitFunction: Parser[EvaluationExpression] = metricSource ~ valueComparison ^^ {
    case source ~ comparison => EvaluateMetric(source, HeadFunction(comparison))
  }

  def evaluationExpression: Parser[EvaluationExpression] = minFunction | maxFunction | meanFunction | implicitFunction

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

  /* the entry point */
  val timeseriesEvaluation: Parser[EvaluationExpression] = _log(orOperator)("timeseriesEvaluation")

  def parseTimeseriesEvaluation(input: String): TimeseriesEvaluation = parseAll(timeseriesEvaluation, input) match {
    case Success(expression: EvaluationExpression, _) => new TimeseriesEvaluation(expression, input)
    case Success(other, _) => throw new Exception("unexpected parse result")
    case failure : NoSuccess => throw new Exception("failed to parse TimeseriesEvaluation: " + failure.msg)
  }
}

object TimeseriesEvaluationParser {
  val parser = new TimeseriesEvaluationParser
  def parseTimeseriesEvaluation(input: String): TimeseriesEvaluation = parser.parseTimeseriesEvaluation(input)
}
