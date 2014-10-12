package io.mandelbrot.core.metrics

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

import scala.math.BigDecimal

class MetricsEvaluationParserSpec extends WordSpec with MustMatchers {

  "MetricsEvaluationParser" must {

    val bareSource = MetricSource("foo")
    val qualifiedSource = MetricSource("/probe:foo")

    "parse 'when foo == 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo == 0")
      println(evaluation.expression)
      evaluation.expression must be === EvaluateSource(bareSource, HeadFunction(ValueEquals(BigDecimal(0))))
    }

    "parse 'when foo != 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo != 0")
      println(evaluation.expression)
      evaluation.expression must be === EvaluateSource(bareSource, HeadFunction(ValueNotEquals(BigDecimal(0))))
    }

    "parse 'when foo < 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo < 0")
      println(evaluation.expression)
      evaluation.expression must be === EvaluateSource(bareSource, HeadFunction(ValueLessThan(BigDecimal(0))))
    }

    "parse 'when foo > 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo > 0")
      println(evaluation.expression)
      evaluation.expression must be === EvaluateSource(bareSource, HeadFunction(ValueGreaterThan(BigDecimal(0))))
    }

    "parse 'when foo.head > 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo.head > 0")
      println(evaluation.expression)
      evaluation.expression must be === EvaluateSource(bareSource, HeadFunction(ValueGreaterThan(BigDecimal(0))))
    }

    "parse 'when foo.each > 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo.each > 0")
      println(evaluation.expression)
      evaluation.expression must be === EvaluateSource(bareSource, EachFunction(ValueGreaterThan(BigDecimal(0))))
    }

    "parse 'when foo.mean > 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo.mean > 0")
      println(evaluation.expression)
      evaluation.expression must be === EvaluateSource(bareSource, MeanFunction(ValueGreaterThan(BigDecimal(0))))
    }

    "parse 'when /probe:foo > 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when /probe:foo > 0")
      println(evaluation.expression)
      evaluation.expression must be === EvaluateSource(qualifiedSource, HeadFunction(ValueGreaterThan(BigDecimal(0))))
    }

    "parse 'when /probe:foo.each > 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when /probe:foo.mean > 0")
      println(evaluation.expression)
      evaluation.expression must be === EvaluateSource(qualifiedSource, MeanFunction(ValueGreaterThan(BigDecimal(0))))
    }
  }
}
