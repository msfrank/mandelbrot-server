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

  def metricName: Parser[String] = regex("[a-zA-Z][a-zA-Z0-9_]*".r)

  def metricSource: Parser[MetricSource] = checkId ~ literal(":") ~ metricName ^^ {
    case (checkId: CheckId) ~ ":" ~ (metricName: String) => MetricSource(checkId, metricName)
  }

  def headFunction: Parser[EvaluationExpression] = metricSource ~ literal(".") ~ literal("head") ~ valueComparison ^^ {
    case source ~ "." ~ "head" ~ comparison => EvaluateMetric(source, HeadFunction(comparison))
  }

  def eachFunction: Parser[EvaluationExpression] = metricSource ~ literal(".") ~ literal("each") ~ valueComparison ^^ {
    case source ~ "." ~ "each" ~ comparison => EvaluateMetric(source, EachFunction(comparison))
  }

  def meanFunction: Parser[EvaluationExpression] = metricSource ~ literal(".") ~ literal("mean") ~ valueComparison ^^ {
    case source ~ "." ~ "mean" ~ comparison => EvaluateMetric(source, MeanFunction(comparison))
  }

  def implicitFunction: Parser[EvaluationExpression] = metricSource ~ valueComparison ^^ {
    case source ~ comparison => EvaluateMetric(source, HeadFunction(comparison))
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

  def parseTimeseriesEvaluation(input: String): TimeseriesEvaluation = parseAll(metricsEvaluation, input) match {
    case Success(expression: EvaluationExpression, _) => new TimeseriesEvaluation(expression, input)
    case Success(other, _) => throw new Exception("unexpected parse result")
    case failure : NoSuccess => throw new Exception("failed to parse TimeseriesEvaluation: " + failure.msg)
  }
}

object TimeseriesEvaluationParser {
  val parser = new TimeseriesEvaluationParser
  def parseTimeseriesEvaluation(input: String): TimeseriesEvaluation = parser.parseTimeseriesEvaluation(input)
}
