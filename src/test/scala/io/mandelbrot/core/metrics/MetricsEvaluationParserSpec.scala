package io.mandelbrot.core.metrics

import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers

import scala.math.BigDecimal

import io.mandelbrot.core.model._

class MetricsEvaluationParserSpec extends WordSpec with ShouldMatchers {

  "MetricsEvaluationParser" should {

    val source = MetricSource("foo.check:value")

    "parse 'when foo.check:value == 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo.check:value == 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateSource(source, HeadFunction(ValueEquals(BigDecimal(0))))
    }

    "parse 'when foo.check:value != 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo.check:value != 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateSource(source, HeadFunction(ValueNotEquals(BigDecimal(0))))
    }

    "parse 'when foo.check:value < 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo.check:value < 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateSource(source, HeadFunction(ValueLessThan(BigDecimal(0))))
    }

    "parse 'when foo.check:value > 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo.check:value > 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateSource(source, HeadFunction(ValueGreaterThan(BigDecimal(0))))
    }

    "parse 'when foo.check:value.head > 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo.check:value.head > 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateSource(source, HeadFunction(ValueGreaterThan(BigDecimal(0))))
    }

    "parse 'when foo.check:value.each > 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo.check:value.each > 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateSource(source, EachFunction(ValueGreaterThan(BigDecimal(0))))
    }

    "parse 'when foo.check:value.mean > 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo.check:value.mean > 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateSource(source, MeanFunction(ValueGreaterThan(BigDecimal(0))))
    }

  }
}
