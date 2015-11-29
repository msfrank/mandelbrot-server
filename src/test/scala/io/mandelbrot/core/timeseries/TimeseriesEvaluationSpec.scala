package io.mandelbrot.core.timeseries

import io.mandelbrot.core.model._
import io.mandelbrot.core.parser.TimeseriesEvaluationParser
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{ShouldMatchers, WordSpec}

import scala.math.BigDecimal

class TimeseriesEvaluationSpec extends WordSpec with ShouldMatchers {

  "TimeseriesEvaluation" when {

    val probeId = ProbeId("foo.source")
    val metricName = "foovalue"
    val dimension = Dimension("agent", "foo.agent")
    val statistic = MetricMinimum
    val source = MetricSource(probeId, metricName, statistic, PerMinute, dimension)

    val oneSampleOptions = TimeseriesEvaluationParser.oneSampleOptions
    val fiveSampleOptions = EvaluationOptions(windowSize = 5, windowUnit = WindowSamples)

    def makeProbeMetrics(statistics: Map[Statistic, Double]): ProbeMetrics = {
      ProbeMetrics(probeId, metricName, dimension, Timestamp(), statistics)
    }

    "evaluating multiple samples" should {

      "evaluate MIN() function" in {
        val evaluation = new TimeseriesEvaluation(EvaluateMetric(source, MinFunction(NumericValueGreaterThan(10.toDouble)), fiveSampleOptions), "")
        val timeseries = new TimeseriesStore(evaluation)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 11.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 12.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 13.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 14.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 15.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 10.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
      }

      "evaluate MAX() function" in {
        val evaluation = new TimeseriesEvaluation(EvaluateMetric(source, MaxFunction(NumericValueLessThan(20.toDouble)), fiveSampleOptions), "")
        val timeseries = new TimeseriesStore(evaluation)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 11.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 12.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 13.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 14.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 15.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 20.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
      }

      "evaluate AVG() function" in {
        val evaluation = new TimeseriesEvaluation(EvaluateMetric(source, MeanFunction(NumericValueLessThan(20.toDouble)), fiveSampleOptions), "")
        val timeseries = new TimeseriesStore(evaluation)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 11.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 12.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 13.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 14.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 15.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 1000.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
      }
    }

    "evaluating a single sample" should {

      "evaluate ==" in {
        val evaluation = new TimeseriesEvaluation(EvaluateMetric(source, HeadFunction(NumericValueEquals(10.toDouble)), oneSampleOptions), "")
        val timeseries = new TimeseriesStore(evaluation)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 10.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 11.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
      }

      "evaluate !=" in {
        val evaluation = new TimeseriesEvaluation(EvaluateMetric(source, HeadFunction(NumericValueNotEquals(10.toDouble)), oneSampleOptions), "")
        val timeseries = new TimeseriesStore(evaluation)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 10.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 11.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
      }

      "evaluate <" in {
        val evaluation = new TimeseriesEvaluation(EvaluateMetric(source, HeadFunction(NumericValueLessThan(10.toDouble)), oneSampleOptions), "")
        val timeseries = new TimeseriesStore(evaluation)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 5.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 10.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 15.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
      }

      "evaluate >" in {
        val evaluation = new TimeseriesEvaluation(EvaluateMetric(source, HeadFunction(NumericValueGreaterThan(10.toDouble)), oneSampleOptions), "")
        val timeseries = new TimeseriesStore(evaluation)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 5.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 10.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 15.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
      }

      "evaluate <=" in {
        val evaluation = new TimeseriesEvaluation(EvaluateMetric(source, HeadFunction(NumericValueLessEqualThan(10.toDouble)), oneSampleOptions), "")
        val timeseries = new TimeseriesStore(evaluation)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 5.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 10.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 15.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
      }

      "evaluate >=" in {
        val evaluation = new TimeseriesEvaluation(EvaluateMetric(source, HeadFunction(NumericValueGreaterEqualThan(10.toDouble)), oneSampleOptions), "")
        val timeseries = new TimeseriesStore(evaluation)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 5.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 10.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.put(source, makeProbeMetrics(Map(MetricMinimum -> 15.toDouble)))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
      }
    }
  }
}
