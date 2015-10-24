package io.mandelbrot.core.parser

import java.util

import io.mandelbrot.core.timeseries._
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

    var frameNumber = 0
    val optionsStack = new util.ArrayDeque[EvaluationOptions]()

    def pushOptions(options: EvaluationOptions): Unit = {
      optionsStack.push(options)
      logger.debug(s"pushing $options onto frame $frameNumber")
    }
    def popOptions(): Unit = {
      logger.debug(s"popping frame $frameNumber")
      optionsStack.pop()
    }

    def getOptions(explicit: Option[EvaluationOptions] = None): EvaluationOptions = explicit.getOrElse {
      optionsStack.peek() match {
        case null => defaultOptions
        case topOfStack => topOfStack
      }
    }
  }

  val logger = LoggerFactory.getLogger(classOf[TimeseriesEvaluationParser])

  /**
   * log runtime parsing information using a single Logger, to make it easier
   * to control logging output.  shamelessly copied from Parsers.scala.
   */
  def _log[T](p: => Parser[T])(name: String): Parser[T] = Parser { in =>
    logger.debug("trying " + name + " at "+ in)
    val r = p(in)
    logger.debug(name + " --> " + r)
    r
  }

  /**
   * track the group frame, for lazy resolution of evaluation options.
   */
  def setFrame[T](p: => Parser[T])(implicit context: Context) = Parser { in =>
    context.frameNumber = context.frameNumber + 1
    logger.info(s"entering frame ${context.frameNumber}")
    val r = p(in)
    logger.info(s"exiting frame ${context.frameNumber}")
    context.frameNumber = context.frameNumber - 1
    r
  }

  /*
   *
   */
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

  /*
   *
   */
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
  def windowUnit: Parser[WindowUnit] = (literal("SAMPLES") | literal("SAMPLE")) ^^ {
    case "SAMPLES" => WindowSamples
    case "SAMPLE" => WindowSamples
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
      EvaluateMetric(source, MinFunction(comparison), specifiedOptions.getOrElse(LazyOptions))
  }

  def maxFunction(implicit context: Context): Parser[EvaluationExpression] = literal("MAX") ~ literal("(") ~ metricSource ~ literal(")") ~ valueComparison ~ options ^^ {
    case "MAX" ~ "(" ~ source ~ ")" ~ comparison ~ specifiedOptions =>
      EvaluateMetric(source, MaxFunction(comparison), context.getOptions(specifiedOptions))
  }

  def meanFunction(implicit context: Context): Parser[EvaluationExpression] = literal("AVG") ~ literal("(") ~ metricSource ~ literal(")") ~ valueComparison ~ options ^^ {
    case "AVG" ~ "(" ~ source ~ ")" ~ comparison ~ specifiedOptions =>
      EvaluateMetric(source, MeanFunction(comparison), context.getOptions(specifiedOptions))
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
   * <Group>        ::= '(' <OrOperator> ')' [<EvaluationOptions>] | <Expression>
   */

  def groupOperator(implicit context: Context): Parser[EvaluationExpression] = _log(setFrame((literal("(") ~> orOperator <~ literal(")")) ~ options))("groupOperator") ^^ {
    case (expression: EvaluationExpression) ~ None => expression
    case (expression: EvaluationExpression) ~ Some(options: EvaluationOptions) =>
      TimeseriesEvaluationParser.resolveLazyOptions(expression, options)
  }

  def groupWrapper(implicit context: Context): Parser[EvaluationExpression] = groupOperator | evaluationExpression

  def notOperator(implicit context: Context): Parser[EvaluationExpression] = _log(("NOT" ~ notOperator) | groupWrapper)("notOperator") ^^ {
    case "NOT" ~ (not: EvaluationExpression) => LogicalNot(not)
    case group: EvaluationExpression => group
  }

  def andOperator(implicit context: Context): Parser[EvaluationExpression] = _log(notOperator ~ rep("AND" ~ notOperator))("andOperator") ^^ {
    case not1 ~ nots if nots.isEmpty =>
      not1
    case not1 ~ nots =>
      val children: Vector[EvaluationExpression] = nots.map { case "AND" ~ group => group }.toVector
      LogicalAnd(not1 +: children)
  }

  def orOperator(implicit context: Context): Parser[EvaluationExpression] = _log(andOperator ~ rep("OR" ~ andOperator))("orOperator") ^^ {
    case and1 ~ ands if ands.isEmpty =>
      and1
    case and1 ~ ands =>
      val children: Vector[EvaluationExpression] = ands.map { case "OR" ~ group => group }.toVector
      LogicalOr(and1 +: children)
  }

  /* the parser entry point */
  def timeseriesEvaluation(implicit context: Context): Parser[EvaluationExpression] = _log(setFrame(orOperator))("timeseriesEvaluation")

  /**
   * Parse the specified input string, and return the in-memory representation
   * of the evaluation expression.
   */
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

  /**
   * Parse the specified input string, and return the in-memory representation
   * of the evaluation expression.  uses the global default parser which is configured
   * with default options.
   */
  def parseTimeseriesEvaluation(input: String): TimeseriesEvaluation = parser.parseTimeseriesEvaluation(input)

  /**
   *
   */
  def applyOptions(expression: EvaluationExpression, options: EvaluationOptions) = expression match {
    case metric: EvaluateMetric =>
      if (metric.options.equals(LazyOptions)) metric.copy(options = options) else metric
    case group: LogicalGrouping =>
      resolveLazyOptions(group, options)
    case _: EvaluationExpression => expression   // just pass child through by default
  }

  /**
   *
   */
  def resolveLazyOptions(expression: EvaluationExpression, options: EvaluationOptions): EvaluationExpression = expression match {
    case and: LogicalAnd =>
      and.copy(children = and.children.map(child => applyOptions(child, options)))
    case or: LogicalOr =>
      or.copy(children = or.children.map(child => applyOptions(child, options)))
    case not: LogicalNot =>
      not.copy(child = applyOptions(not.child, options))
    case _ => applyOptions(expression, options)
  }
 }
