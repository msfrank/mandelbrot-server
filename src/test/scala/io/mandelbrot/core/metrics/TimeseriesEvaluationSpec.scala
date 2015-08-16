package io.mandelbrot.core.metrics

import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers

import scala.math.BigDecimal

import io.mandelbrot.core.model._

class TimeseriesEvaluationSpec extends WordSpec with ShouldMatchers {

  "TimeseriesEvaluation" should {

    val source = MetricSource(CheckId("foo.source"), "foovalue")

    def makeCheckStatus(metrics: Map[String,BigDecimal]): CheckStatus = {
      CheckStatus(0, DateTime.now(DateTimeZone.UTC), CheckKnown, None, CheckHealthy, metrics, None, None, None, None, false)
    }

    "evaluate ==" in {
      val metrics = new TimeseriesStore()
      val evaluation = new TimeseriesEvaluation(EvaluateMetric(source, HeadFunction(NumericValueEquals(BigDecimal(10)))), "")
      metrics.resize(evaluation)
      metrics.append(source.checkId, makeCheckStatus(Map("foovalue" -> BigDecimal(10))))
      evaluation.evaluate(metrics) shouldEqual Some(true)
      metrics.append(source.checkId, makeCheckStatus(Map("foovalue" -> BigDecimal(11))))
      evaluation.evaluate(metrics) shouldEqual Some(false)
    }

    "evaluate !=" in {
      val metrics = new TimeseriesStore()
      val evaluation = new TimeseriesEvaluation(EvaluateMetric(source, HeadFunction(NumericValueNotEquals(BigDecimal(10)))), "")
      metrics.resize(evaluation)
      metrics.append(source.checkId, makeCheckStatus(Map("foovalue" -> BigDecimal(10))))
      evaluation.evaluate(metrics) shouldEqual Some(false)
      metrics.append(source.checkId, makeCheckStatus(Map("foovalue" -> BigDecimal(11))))
      evaluation.evaluate(metrics) shouldEqual Some(true)
    }

    "evaluate <" in {
      val metrics = new TimeseriesStore()
      val evaluation = new TimeseriesEvaluation(EvaluateMetric(source, HeadFunction(NumericValueLessThan(BigDecimal(10)))), "")
      metrics.resize(evaluation)
      metrics.append(source.checkId, makeCheckStatus(Map("foovalue" -> BigDecimal(5))))
      evaluation.evaluate(metrics) shouldEqual Some(true)
      metrics.append(source.checkId, makeCheckStatus(Map("foovalue" -> BigDecimal(10))))
      evaluation.evaluate(metrics) shouldEqual Some(false)
      metrics.append(source.checkId, makeCheckStatus(Map("foovalue" -> BigDecimal(15))))
      evaluation.evaluate(metrics) shouldEqual Some(false)
    }

    "evaluate >" in {
      val metrics = new TimeseriesStore()
      val evaluation = new TimeseriesEvaluation(EvaluateMetric(source, HeadFunction(NumericValueGreaterThan(BigDecimal(10)))), "")
      metrics.resize(evaluation)
      metrics.append(source.checkId, makeCheckStatus(Map("foovalue" -> BigDecimal(5))))
      evaluation.evaluate(metrics) shouldEqual Some(false)
      metrics.append(source.checkId, makeCheckStatus(Map("foovalue" -> BigDecimal(10))))
      evaluation.evaluate(metrics) shouldEqual Some(false)
      metrics.append(source.checkId, makeCheckStatus(Map("foovalue" -> BigDecimal(15))))
      evaluation.evaluate(metrics) shouldEqual Some(true)
    }

    "evaluate <=" in {
      val metrics = new TimeseriesStore()
      val evaluation = new TimeseriesEvaluation(EvaluateMetric(source, HeadFunction(NumericValueLessEqualThan(BigDecimal(10)))), "")
      metrics.resize(evaluation)
      metrics.append(source.checkId, makeCheckStatus(Map("foovalue" -> BigDecimal(5))))
      evaluation.evaluate(metrics) shouldEqual Some(true)
      metrics.append(source.checkId, makeCheckStatus(Map("foovalue" -> BigDecimal(10))))
      evaluation.evaluate(metrics) shouldEqual Some(true)
      metrics.append(source.checkId, makeCheckStatus(Map("foovalue" -> BigDecimal(15))))
      evaluation.evaluate(metrics) shouldEqual Some(false)
    }

    "evaluate >=" in {
      val metrics = new TimeseriesStore()
      val evaluation = new TimeseriesEvaluation(EvaluateMetric(source, HeadFunction(NumericValueGreaterEqualThan(BigDecimal(10)))), "")
      metrics.resize(evaluation)
      metrics.append(source.checkId, makeCheckStatus(Map("foovalue" -> BigDecimal(5))))
      evaluation.evaluate(metrics) shouldEqual Some(false)
      metrics.append(source.checkId, makeCheckStatus(Map("foovalue" -> BigDecimal(10))))
      evaluation.evaluate(metrics) shouldEqual Some(true)
      metrics.append(source.checkId, makeCheckStatus(Map("foovalue" -> BigDecimal(15))))
      evaluation.evaluate(metrics) shouldEqual Some(true)
    }
  }
}
