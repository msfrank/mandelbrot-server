package io.mandelbrot.core.parser

import io.mandelbrot.core.metrics._
import io.mandelbrot.core.model._
import io.mandelbrot.core.timeseries._
import org.scalatest.{ShouldMatchers, WordSpec}

import scala.math.BigDecimal

class TimeseriesEvaluationParserSpec extends WordSpec with ShouldMatchers {

  val oneSampleOptions = TimeseriesEvaluationParser.oneSampleOptions
  val twoSampleOptions = EvaluationOptions(windowSize = 2, windowUnit = WindowSamples)
  val fiveSampleOptions = EvaluationOptions(windowSize = 5, windowUnit = WindowSamples)

  "A TimeseriesEvaluationParser" when {

    "parsing an evaluation mapping a condition over the last data point" should {

      val source = MetricSource("probe:system.load:load1:p99:1minute:host=foo.com")

      "parse 'probe:system.load:load1:p99:1minute:host=foo.com == 0'" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("probe:system.load:load1:p99:1minute:host=foo.com == 0")
        println(evaluation.expression)
        evaluation.expression shouldEqual EvaluateMetric(source, HeadFunction(NumericValueEquals(0)), oneSampleOptions)
      }

      "parse 'probe:system.load:load1:p99:1minute:host=foo.com != 0'" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("probe:system.load:load1:p99:1minute:host=foo.com != 0")
        println(evaluation.expression)
        evaluation.expression shouldEqual EvaluateMetric(source, HeadFunction(NumericValueNotEquals(0)), oneSampleOptions)
      }

      "parse 'probe:system.load:load1:p99:1minute:host=foo.com < 0'" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("probe:system.load:load1:p99:1minute:host=foo.com < 0")
        println(evaluation.expression)
        evaluation.expression shouldEqual EvaluateMetric(source, HeadFunction(NumericValueLessThan(0)), oneSampleOptions)
      }

      "parse 'probe:system.load:load1:p99:1minute:host=foo.com > 0'" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("probe:system.load:load1:p99:1minute:host=foo.com > 0")
        println(evaluation.expression)
        evaluation.expression shouldEqual EvaluateMetric(source, HeadFunction(NumericValueGreaterThan(0)), oneSampleOptions)
      }

    }

    "parsing an evaluation mapping a condition over a timeseries" should {

      val source = MetricSource("probe:system.load:load1:p99:1minute:host=foo.com")

      "parse 'MIN(probe:system.load:load1:p99:1minute:host=foo.com) > 0 OVER 5 SAMPLES'" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("MIN(probe:system.load:load1:p99:1minute:host=foo.com) > 0 OVER 5 SAMPLES")
        println(evaluation.expression)
        evaluation.expression shouldEqual EvaluateMetric(source, MinFunction(NumericValueGreaterThan(0)), fiveSampleOptions)
      }

      "parse 'MAX(probe:system.load:load1:p99:1minute:host=foo.com) > 0 OVER 5 SAMPLES'" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("MAX(probe:system.load:load1:p99:1minute:host=foo.com) > 0 OVER 5 SAMPLES")
        println(evaluation.expression)
        evaluation.expression shouldEqual EvaluateMetric(source, MaxFunction(NumericValueGreaterThan(0)), fiveSampleOptions)
      }

      "parse 'AVG(probe:system.load:load1:p99:1minute:host=foo.com) > 0 OVER 5 SAMPLES'" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("AVG(probe:system.load:load1:p99:1minute:host=foo.com) > 0 OVER 5 SAMPLES")
        println(evaluation.expression)
        evaluation.expression shouldEqual EvaluateMetric(source, MeanFunction(NumericValueGreaterThan(0)), fiveSampleOptions)
      }
    }

    "parsing an evaluation with grouping operators" should {

      val metric1 = MetricSource("probe:system.load:load1:p99:1minute:host=foo.com")
      val metric2 = MetricSource("probe:system.load:load5:p99:1minute:host=foo.com")

      "parse 'MIN(probe:system.load:load1:p99:1minute:host=foo.com) > 0 OVER 2 SAMPLES OR MIN(probe:system.load:load5:p99:1minute:host=foo.com) > 1 OVER 5 SAMPLES" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("MIN(probe:system.load:load1:p99:1minute:host=foo.com) > 0 OVER 2 SAMPLES OR MIN(probe:system.load:load5:p99:1minute:host=foo.com) > 1 OVER 5 SAMPLES")
        println(evaluation.expression)
        evaluation.expression shouldEqual LogicalOr(Vector(
          EvaluateMetric(metric1, MinFunction(NumericValueGreaterThan(0)), twoSampleOptions),
          EvaluateMetric(metric2, MinFunction(NumericValueGreaterThan(1)), fiveSampleOptions)
        ))
      }

      "parse 'MIN(probe:system.load:load1:p99:1minute:host=foo.com) > 0 OVER 2 SAMPLES AND MIN(probe:system.load:load5:p99:1minute:host=foo.com) > 1 OVER 5 SAMPLES" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("MIN(probe:system.load:load1:p99:1minute:host=foo.com) > 0 OVER 2 SAMPLES AND MIN(probe:system.load:load5:p99:1minute:host=foo.com) > 1 OVER 5 SAMPLES")
        println(evaluation.expression)
        evaluation.expression shouldEqual LogicalAnd(Vector(
          EvaluateMetric(metric1, MinFunction(NumericValueGreaterThan(0)), twoSampleOptions),
          EvaluateMetric(metric2, MinFunction(NumericValueGreaterThan(1)), fiveSampleOptions)
        ))
      }

      "parse 'NOT MIN(probe:system.load:load1:p99:1minute:host=foo.com) > 0 OVER 5 SAMPLES" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("NOT MIN(probe:system.load:load1:p99:1minute:host=foo.com) > 0 OVER 5 SAMPLES")
        println(evaluation.expression)
        evaluation.expression shouldEqual LogicalNot(EvaluateMetric(metric1, MinFunction(NumericValueGreaterThan(0)), fiveSampleOptions))
      }

      "parse 'MIN(probe:system.load:load1:p99:1minute:host=foo.com) > 0 OVER 2 SAMPLES AND NOT MIN(probe:system.load:load1:p99:1minute:host=foo.com) > 1 OVER 5 SAMPLES" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("MIN(probe:system.load:load1:p99:1minute:host=foo.com) > 0 OVER 2 SAMPLES AND NOT MIN(probe:system.load:load1:p99:1minute:host=foo.com) > 1 OVER 5 SAMPLES")
        println(evaluation.expression)
        evaluation.expression shouldEqual LogicalAnd(Vector(
          EvaluateMetric(metric1, MinFunction(NumericValueGreaterThan(0)), twoSampleOptions),
          LogicalNot(EvaluateMetric(metric1, MinFunction(NumericValueGreaterThan(1)), fiveSampleOptions))
        ))
      }

      "parse '(MIN(probe:system.load:load1:p99:1minute:host=foo.com) > 0 OR MIN(probe:system.load:load5:p99:1minute:host=foo.com) > 1) OVER 5 SAMPLES" in {
        val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation("(MIN(probe:system.load:load1:p99:1minute:host=foo.com) > 0 OR MIN(probe:system.load:load5:p99:1minute:host=foo.com) > 1) OVER 5 SAMPLES")
        println(evaluation.expression)
        evaluation.expression shouldEqual LogicalOr(Vector(
          EvaluateMetric(metric1, MinFunction(NumericValueGreaterThan(0)), fiveSampleOptions),
          EvaluateMetric(metric2, MinFunction(NumericValueGreaterThan(1)), fiveSampleOptions)
        ))
      }
    }

    "parsing a metric source with a single segment" should {

      "parse 'probe:system.load:load1:p99:1minute:host=foo.com'" in {
        val parser = TimeseriesEvaluationParser.parser
        val source = parser.parseAll(parser.metricSource, "probe:metric:name:p99:1minute:host=foo.com").get
        println(source)
        source shouldEqual MetricSource(ProbeId("metric"), "name", Metric99thPercentile, PerMinute, Dimension("host", "foo.com"))
      }

    }

    "parsing a metric source with multiple segments" should {

      "parse 'probe:nested.probe.id:value'" in {
        val parser = TimeseriesEvaluationParser.parser
        val source = parser.parseAll(parser.metricSource, "probe:system.cpu.cpu0:sys:maximum:1minute:host=bar.com").get
        println(source)
        source shouldEqual MetricSource(ProbeId("system.cpu.cpu0"), "sys", MetricMaximum, PerMinute, Dimension("host", "bar.com"))
      }
    }
  }
}
