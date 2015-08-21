package io.mandelbrot.core.parser

import io.mandelbrot.core.metrics._
import io.mandelbrot.core.model._
import org.scalatest.{ShouldMatchers, WordSpec}

import scala.math.BigDecimal

class TimeseriesEvaluationParserSpec extends WordSpec with ShouldMatchers {

  "TimeseriesEvaluationParser" should {

    "parse 'when check:value == 0'" in {
      val source = MetricSource("check:value")
      val evaluation = new TimeseriesEvaluationParser().parseTimeseriesEvaluation("when check:value == 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateMetric(source, HeadFunction(NumericValueEquals(BigDecimal(0))))
    }

    "parse 'when check:value != 0'" in {
      val source = MetricSource("check:value")
      val evaluation = new TimeseriesEvaluationParser().parseTimeseriesEvaluation("when check:value != 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateMetric(source, HeadFunction(NumericValueNotEquals(BigDecimal(0))))
    }

    "parse 'when check:value < 0'" in {
      val source = MetricSource("check:value")
      val evaluation = new TimeseriesEvaluationParser().parseTimeseriesEvaluation("when check:value < 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateMetric(source, HeadFunction(NumericValueLessThan(BigDecimal(0))))
    }

    "parse 'when check:value > 0'" in {
      val source = MetricSource("check:value")
      val evaluation = new TimeseriesEvaluationParser().parseTimeseriesEvaluation("when check:value > 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateMetric(source, HeadFunction(NumericValueGreaterThan(BigDecimal(0))))
    }

    "parse 'when check:value.head > 0'" in {
      val source = MetricSource("check:value")
      val evaluation = new TimeseriesEvaluationParser().parseTimeseriesEvaluation("when check:value.head > 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateMetric(source, HeadFunction(NumericValueGreaterThan(BigDecimal(0))))
    }

    "parse 'when check:value.each > 0'" in {
      val source = MetricSource("check:value")
      val evaluation = new TimeseriesEvaluationParser().parseTimeseriesEvaluation("when check:value.each > 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateMetric(source, EachFunction(NumericValueGreaterThan(BigDecimal(0))))
    }

    "parse 'when check:value.mean > 0'" in {
      val source = MetricSource("check:value")
      val evaluation = new TimeseriesEvaluationParser().parseTimeseriesEvaluation("when check:value.mean > 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateMetric(source, MeanFunction(NumericValueGreaterThan(BigDecimal(0))))
    }

    "parse 'when foo.check:value == 0'" in {
      val source = MetricSource("foo.check:value")
      val evaluation = new TimeseriesEvaluationParser().parseTimeseriesEvaluation("when foo.check:value == 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateMetric(source, HeadFunction(NumericValueEquals(BigDecimal(0))))
    }

    "parse 'when foo.check:value != 0'" in {
      val source = MetricSource("foo.check:value")
      val evaluation = new TimeseriesEvaluationParser().parseTimeseriesEvaluation("when foo.check:value != 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateMetric(source, HeadFunction(NumericValueNotEquals(BigDecimal(0))))
    }

    "parse 'when foo.check:value < 0'" in {
      val source = MetricSource("foo.check:value")
      val evaluation = new TimeseriesEvaluationParser().parseTimeseriesEvaluation("when foo.check:value < 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateMetric(source, HeadFunction(NumericValueLessThan(BigDecimal(0))))
    }

    "parse 'when foo.check:value > 0'" in {
      val source = MetricSource("foo.check:value")
      val evaluation = new TimeseriesEvaluationParser().parseTimeseriesEvaluation("when foo.check:value > 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateMetric(source, HeadFunction(NumericValueGreaterThan(BigDecimal(0))))
    }

    "parse 'when foo.check:value.head > 0'" in {
      val source = MetricSource("foo.check:value")
      val evaluation = new TimeseriesEvaluationParser().parseTimeseriesEvaluation("when foo.check:value.head > 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateMetric(source, HeadFunction(NumericValueGreaterThan(BigDecimal(0))))
    }

    "parse 'when foo.check:value.each > 0'" in {
      val source = MetricSource("foo.check:value")
      val evaluation = new TimeseriesEvaluationParser().parseTimeseriesEvaluation("when foo.check:value.each > 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateMetric(source, EachFunction(NumericValueGreaterThan(BigDecimal(0))))
    }

    "parse 'when foo.check:value.mean > 0'" in {
      val source = MetricSource("foo.check:value")
      val evaluation = new TimeseriesEvaluationParser().parseTimeseriesEvaluation("when foo.check:value.mean > 0")
      println(evaluation.expression)
      evaluation.expression shouldEqual EvaluateMetric(source, MeanFunction(NumericValueGreaterThan(BigDecimal(0))))
    }

  }
}
