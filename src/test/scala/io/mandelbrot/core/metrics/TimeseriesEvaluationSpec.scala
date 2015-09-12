package io.mandelbrot.core.metrics

import io.mandelbrot.core.parser.TimeseriesEvaluationParser
import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers

import scala.math.BigDecimal

import io.mandelbrot.core.model._

class TimeseriesEvaluationSpec extends WordSpec with ShouldMatchers {

  "TimeseriesEvaluation" when {

    val metricName = "foovalue"
    val metric = MetricSource(ProbeId("foo.source"), metricName)
    val source = metric.toObservationSource
    val options = TimeseriesEvaluationParser.oneSampleOptions

    def makeObservation(timeseries: Map[String, BigDecimal]): Observation = ScalarMapObservation(DateTime.now(DateTimeZone.UTC), timeseries)

    "evaluating a single sample" should {
      "evaluate ==" in {
        val timeseries = new TimeseriesStore()
        val evaluation = new TimeseriesEvaluation(EvaluateMetric(metric, HeadFunction(NumericValueEquals(BigDecimal(10))), options), "")
        timeseries.resize(evaluation)
        timeseries.append(source, makeObservation(Map(metricName -> BigDecimal(10))))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.append(source, makeObservation(Map(metricName -> BigDecimal(11))))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
      }

      "evaluate !=" in {
        val timeseries = new TimeseriesStore()
        val evaluation = new TimeseriesEvaluation(EvaluateMetric(metric, HeadFunction(NumericValueNotEquals(BigDecimal(10))), options), "")
        timeseries.resize(evaluation)
        timeseries.append(source, makeObservation(Map(metricName -> BigDecimal(10))))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
        timeseries.append(source, makeObservation(Map(metricName -> BigDecimal(11))))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
      }

      "evaluate <" in {
        val timeseries = new TimeseriesStore()
        val evaluation = new TimeseriesEvaluation(EvaluateMetric(metric, HeadFunction(NumericValueLessThan(BigDecimal(10))), options), "")
        timeseries.resize(evaluation)
        timeseries.append(source, makeObservation(Map(metricName -> BigDecimal(5))))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.append(source, makeObservation(Map(metricName -> BigDecimal(10))))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
        timeseries.append(source, makeObservation(Map(metricName -> BigDecimal(15))))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
      }

      "evaluate >" in {
        val timeseries = new TimeseriesStore()
        val evaluation = new TimeseriesEvaluation(EvaluateMetric(metric, HeadFunction(NumericValueGreaterThan(BigDecimal(10))), options), "")
        timeseries.resize(evaluation)
        timeseries.append(source, makeObservation(Map(metricName -> BigDecimal(5))))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
        timeseries.append(source, makeObservation(Map(metricName -> BigDecimal(10))))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
        timeseries.append(source, makeObservation(Map(metricName -> BigDecimal(15))))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
      }

      "evaluate <=" in {
        val timeseries = new TimeseriesStore()
        val evaluation = new TimeseriesEvaluation(EvaluateMetric(metric, HeadFunction(NumericValueLessEqualThan(BigDecimal(10))), options), "")
        timeseries.resize(evaluation)
        timeseries.append(source, makeObservation(Map(metricName -> BigDecimal(5))))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.append(source, makeObservation(Map(metricName -> BigDecimal(10))))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.append(source, makeObservation(Map(metricName -> BigDecimal(15))))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
      }

      "evaluate >=" in {
        val timeseries = new TimeseriesStore()
        val evaluation = new TimeseriesEvaluation(EvaluateMetric(metric, HeadFunction(NumericValueGreaterEqualThan(BigDecimal(10))), options), "")
        timeseries.resize(evaluation)
        timeseries.append(source, makeObservation(Map(metricName -> BigDecimal(5))))
        evaluation.evaluate(timeseries) shouldEqual Some(false)
        timeseries.append(source, makeObservation(Map(metricName -> BigDecimal(10))))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
        timeseries.append(source, makeObservation(Map(metricName -> BigDecimal(15))))
        evaluation.evaluate(timeseries) shouldEqual Some(true)
      }
    }
  }
}
