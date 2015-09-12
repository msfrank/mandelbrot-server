package io.mandelbrot.core.parser

import java.util

import org.slf4j.LoggerFactory
import scala.util.parsing.combinator.JavaTokenParsers

import io.mandelbrot.core.model._
import io.mandelbrot.core.metrics._

/**
 *
 */
class TimeseriesEvaluationParser(globalOptions: EvaluationOptions) extends JavaTokenParsers {

  // holds application context each time the parser is run
  private class Context(defaultOptions: EvaluationOptions) {
    val optionsStack = new util.ArrayDeque[EvaluationOptions]()
    def options(explicit: Option[EvaluationOptions] = None): EvaluationOptions = explicit.getOrElse {
      optionsStack.peek() match {
        case null => defaultOptions
        case topOfStack => topOfStack
      }
    }
  }

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
   *
   */
  def windowUnit: Parser[WindowUnit] = literal("SAMPLES") ^^ {
    case "SAMPLES" => WindowSamples
  }

  def evaluationOptions: Parser[EvaluationOptions] = _log(literal("OVER") ~ wholeNumber ~ windowUnit)("evaluationOptions") ^^ {
    case "OVER" ~ (magnitude: String) ~ (windowUnits: WindowUnit) =>
      val windowSize = magnitude.toInt
      if (windowSize < 1)
        throw new Exception("window size must be greater than 0")
      EvaluationOptions(windowSize, windowUnits)
  }

  def options: Parser[Option[EvaluationOptions]] = opt(evaluationOptions)

  /*
   * <MINFunction>  ::= 'MIN' '(' <MetricSource> ')' <ValueComparison>
   * <MAXFunction>  ::= 'MAX' '(' <MetricSource> ')' <ValueComparison>
   * <AVGFunction>  ::= 'AVG' '(' <MetricSource> ')' <ValueComparison>
   */
  def minFunction(implicit context: Context): Parser[EvaluationExpression] = literal("MIN") ~ literal("(") ~ metricSource ~ literal(")") ~ valueComparison ~ options ^^ {
    case "MIN" ~ "(" ~ source ~ ")" ~ comparison ~ specifiedOptions =>
      EvaluateMetric(source, MinFunction(comparison), context.options(specifiedOptions))
  }

  def maxFunction(implicit context: Context): Parser[EvaluationExpression] = literal("MAX") ~ literal("(") ~ metricSource ~ literal(")") ~ valueComparison ~ options ^^ {
    case "MAX" ~ "(" ~ source ~ ")" ~ comparison ~ specifiedOptions =>
      EvaluateMetric(source, MaxFunction(comparison), context.options(specifiedOptions))
  }

  def meanFunction(implicit context: Context): Parser[EvaluationExpression] = literal("AVG") ~ literal("(") ~ metricSource ~ literal(")") ~ valueComparison ~ options ^^ {
    case "AVG" ~ "(" ~ source ~ ")" ~ comparison ~ specifiedOptions =>
      EvaluateMetric(source, MeanFunction(comparison), context.options(specifiedOptions))
  }

  def implicitFunction(implicit context: Context): Parser[EvaluationExpression] = metricSource ~ valueComparison ^^ {
    case source ~ comparison =>
      EvaluateMetric(source, HeadFunction(comparison), TimeseriesEvaluationParser.oneSampleOptions)
  }

  def evaluationExpression(implicit context: Context): Parser[EvaluationExpression] = minFunction | maxFunction | meanFunction | implicitFunction

  /*
   * <Query>        ::= <OrOperator>
   * <OrOperator>   ::= <AndOperator> ('OR' <AndOperator>)*
   * <AndOperator>  ::= <NotOperator> ('AND' <NotOperator>)*
   * <NotOperator>  ::= ['NOT'] <NotOperator> | <Group>
   * <Group>        ::= '(' <OrOperator> ')' | <Expression>
   */

  def groupOperator(implicit context: Context): Parser[EvaluationExpression] = _log((literal("(") ~> orOperator <~ literal(")")) | evaluationExpression)("groupOperator") ^^ {
    case group: EvaluationExpression => group
  }

  def notOperator(implicit context: Context): Parser[EvaluationExpression] = _log(("not" ~ notOperator) | groupOperator)("notOperator") ^^ {
    case "not" ~ (not: EvaluationExpression) => LogicalNot(not)
    case group: EvaluationExpression => group
  }

  def andOperator(implicit context: Context): Parser[EvaluationExpression] = _log(notOperator ~ rep("and" ~ notOperator))("andOperator") ^^ {
    case not1 ~ nots if nots.isEmpty =>
      not1
    case not1 ~ nots =>
      val children: Vector[EvaluationExpression] = nots.map { case "and" ~ group => group }.toVector
      LogicalAnd(not1 +: children)
  }

  def orOperator(implicit context: Context): Parser[EvaluationExpression] = _log(andOperator ~ rep("or" ~ andOperator))("orOperator") ^^ {
    case and1 ~ ands if ands.isEmpty =>
      and1
    case and1 ~ ands =>
      val children: Vector[EvaluationExpression] = ands.map { case "or" ~ group => group }.toVector
      LogicalOr(and1 +: children)
  }

  /* the entry point */
  def timeseriesEvaluation(implicit context: Context): Parser[EvaluationExpression] = _log(orOperator)("timeseriesEvaluation")

  def parseTimeseriesEvaluation(input: String): TimeseriesEvaluation = {
    def entryPoint: Parser[EvaluationExpression] = {
      timeseriesEvaluation(new Context(globalOptions))
    }
    parseAll(entryPoint, input) match {
      case Success(expression: EvaluationExpression, _) => new TimeseriesEvaluation(expression, input)
      case Success(other, _) => throw new Exception("unexpected parse result")
      case failure : NoSuccess => throw new Exception("failed to parse TimeseriesEvaluation: " + failure.msg)
    }
  }
}

object TimeseriesEvaluationParser {
  val globalOptions = EvaluationOptions(windowSize = 1, windowUnits = WindowSamples)
  val oneSampleOptions = EvaluationOptions(windowSize = 1, windowUnits = WindowSamples)
  val parser = new TimeseriesEvaluationParser(globalOptions)
  def parseTimeseriesEvaluation(input: String): TimeseriesEvaluation = parser.parseTimeseriesEvaluation(input)
}
