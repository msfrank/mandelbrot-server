package io.mandelbrot.core.parser

import io.mandelbrot.core.metrics._
import io.mandelbrot.core.model._
import org.scalatest.{ShouldMatchers, WordSpec}

import scala.math.BigDecimal

class TimeseriesEvaluationParserSpec extends WordSpec with ShouldMatchers {

  val oneSampleOptions = TimeseriesEvaluationParser.oneSampleOptions
  val twoSampleOptions = EvaluationOptions(windowSize = 2, windowUnits = WindowSamples)
  val fiveSampleOptions = EvaluationOptions(windowSize = 5, windowUnits = WindowSamples)

  "A TimeseriesEvaluationParser" when {

    "parsing an evaluation mapping a condition over the last data point" should {

      val source = MetricSource("probe:id:value")

      "parse 'probe:id:value == 0'" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("probe:id:value == 0")
        println(evaluation.expression)
        evaluation.expression shouldEqual EvaluateMetric(source, HeadFunction(NumericValueEquals(BigDecimal(0))), oneSampleOptions)
      }

      "parse 'probe:id:value != 0'" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("probe:id:value != 0")
        println(evaluation.expression)
        evaluation.expression shouldEqual EvaluateMetric(source, HeadFunction(NumericValueNotEquals(BigDecimal(0))), oneSampleOptions)
      }

      "parse 'probe:id:value < 0'" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("probe:id:value < 0")
        println(evaluation.expression)
        evaluation.expression shouldEqual EvaluateMetric(source, HeadFunction(NumericValueLessThan(BigDecimal(0))), oneSampleOptions)
      }

      "parse 'probe:id:value > 0'" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("probe:id:value > 0")
        println(evaluation.expression)
        evaluation.expression shouldEqual EvaluateMetric(source, HeadFunction(NumericValueGreaterThan(BigDecimal(0))), oneSampleOptions)
      }

    }

    "parsing an evaluation mapping a condition over a timeseries" should {

      val source = MetricSource("probe:check:value")

      "parse 'MIN(probe:check:value) > 0 OVER 5 SAMPLES'" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("MIN(probe:check:value) > 0 OVER 5 SAMPLES")
        println(evaluation.expression)
        evaluation.expression shouldEqual EvaluateMetric(source, MinFunction(NumericValueGreaterThan(BigDecimal(0))), fiveSampleOptions)
      }

      "parse 'MAX(probe:check:value) > 0 OVER 5 SAMPLES'" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("MAX(probe:check:value) > 0 OVER 5 SAMPLES")
        println(evaluation.expression)
        evaluation.expression shouldEqual EvaluateMetric(source, MaxFunction(NumericValueGreaterThan(BigDecimal(0))), fiveSampleOptions)
      }

      "parse 'AVG(probe:check:value) > 0 OVER 5 SAMPLES'" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("AVG(probe:check:value) > 0 OVER 5 SAMPLES")
        println(evaluation.expression)
        evaluation.expression shouldEqual EvaluateMetric(source, MeanFunction(NumericValueGreaterThan(BigDecimal(0))), fiveSampleOptions)
      }
    }

    "parsing an evaluation with grouping operators" should {

      val metric1 = MetricSource("probe:check:value1")
      val metric2 = MetricSource("probe:check:value2")

      "parse 'MIN(probe:check:value1) > 0 OVER 2 SAMPLES OR MIN(probe:check:value2) > 1 OVER 5 SAMPLES" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("MIN(probe:check:value1) > 0 OVER 2 SAMPLES OR MIN(probe:check:value2) > 1 OVER 5 SAMPLES")
        println(evaluation.expression)
        evaluation.expression shouldEqual LogicalOr(Vector(
          EvaluateMetric(metric1, MinFunction(NumericValueGreaterThan(BigDecimal(0))), twoSampleOptions),
          EvaluateMetric(metric2, MinFunction(NumericValueGreaterThan(BigDecimal(1))), fiveSampleOptions)
        ))
      }

      "parse 'MIN(probe:check:value1) > 0 OVER 2 SAMPLES AND MIN(probe:check:value2) > 1 OVER 5 SAMPLES" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("MIN(probe:check:value1) > 0 OVER 2 SAMPLES AND MIN(probe:check:value2) > 1 OVER 5 SAMPLES")
        println(evaluation.expression)
        evaluation.expression shouldEqual LogicalAnd(Vector(
          EvaluateMetric(metric1, MinFunction(NumericValueGreaterThan(BigDecimal(0))), twoSampleOptions),
          EvaluateMetric(metric2, MinFunction(NumericValueGreaterThan(BigDecimal(1))), fiveSampleOptions)
        ))
      }

      "parse 'NOT MIN(probe:check:value1) > 0 OVER 5 SAMPLES" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("NOT MIN(probe:check:value1) > 0 OVER 5 SAMPLES")
        println(evaluation.expression)
        evaluation.expression shouldEqual LogicalNot(EvaluateMetric(metric1, MinFunction(NumericValueGreaterThan(BigDecimal(0))), fiveSampleOptions))
      }

      "parse 'MIN(probe:check:value1) > 0 OVER 2 SAMPLES AND NOT MIN(probe:check:value1) > 1 OVER 5 SAMPLES" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("MIN(probe:check:value1) > 0 OVER 2 SAMPLES AND NOT MIN(probe:check:value1) > 1 OVER 5 SAMPLES")
        println(evaluation.expression)
        evaluation.expression shouldEqual LogicalAnd(Vector(
          EvaluateMetric(metric1, MinFunction(NumericValueGreaterThan(BigDecimal(0))), twoSampleOptions),
          LogicalNot(EvaluateMetric(metric1, MinFunction(NumericValueGreaterThan(BigDecimal(1))), fiveSampleOptions))
        ))
      }

      "parse '(MIN(probe:check:value1) > 0 OR MIN(probe:check:value2) > 1) OVER 5 SAMPLES" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("(MIN(probe:check:value1) > 0 OR MIN(probe:check:value2) > 1) OVER 5 SAMPLES")
        println(evaluation.expression)
        evaluation.expression shouldEqual LogicalOr(Vector(
          EvaluateMetric(metric1, MinFunction(NumericValueGreaterThan(BigDecimal(0))), fiveSampleOptions),
          EvaluateMetric(metric2, MinFunction(NumericValueGreaterThan(BigDecimal(1))), fiveSampleOptions)
        ))
      }
    }

    "parsing a metric source with a single segment" should {

      "parse 'probe:id:value'" in {
        val parser = TimeseriesEvaluationParser.parser
        val source = parser.parseAll(parser.metricSource, "probe:id:value").get
        println(source)
        source shouldEqual MetricSource("probe:id:value")
      }

    }

    "parsing a metric source with multiple segments" should {

      "parse 'probe:nested.probe.id:value'" in {
        val parser = TimeseriesEvaluationParser.parser
        val source = parser.parseAll(parser.metricSource, "probe:nested.probe.id:value").get
        println(source)
        source shouldEqual MetricSource("probe:nested.probe.id:value")
      }
    }
  }
}
