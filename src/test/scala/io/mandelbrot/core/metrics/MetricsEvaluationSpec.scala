package io.mandelbrot.core.metrics

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

import scala.math.BigDecimal

class MetricsEvaluationSpec extends WordSpec with MustMatchers {

  "MetricsEvaluation" must {

    val source = MetricSource(Vector.empty, "foo")

    "evaluate ==" in {
      val evaluation = new MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueEquals(BigDecimal(10)))), "")
      val metrics = new MetricsStore(evaluation)
      metrics.append(source, BigDecimal(10))
      evaluation.evaluate(metrics) must be(Some(true))
      metrics.append(source, BigDecimal(11))
      evaluation.evaluate(metrics) must be(Some(false))
    }

    "evaluate !=" in {
      val evaluation = new MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueNotEquals(BigDecimal(10)))), "")
      val metrics = new MetricsStore(evaluation)
      metrics.append(source, BigDecimal(10))
      evaluation.evaluate(metrics) must be(Some(false))
      metrics.append(source, BigDecimal(11))
      evaluation.evaluate(metrics) must be(Some(true))
    }

    "evaluate <" in {
      val evaluation = new MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueLessThan(BigDecimal(10)))), "")
      val metrics = new MetricsStore(evaluation)
      metrics.append(source, BigDecimal(5))
      evaluation.evaluate(metrics) must be(Some(true))
      metrics.append(source, BigDecimal(10))
      evaluation.evaluate(metrics) must be(Some(false))
      metrics.append(source, BigDecimal(15))
      evaluation.evaluate(metrics) must be(Some(false))
    }

    "evaluate >" in {
      val evaluation = new MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueGreaterThan(BigDecimal(10)))), "")
      val metrics = new MetricsStore(evaluation)
      metrics.append(source, BigDecimal(5))
      evaluation.evaluate(metrics) must be(Some(false))
      metrics.append(source, BigDecimal(10))
      evaluation.evaluate(metrics) must be(Some(false))
      metrics.append(source, BigDecimal(15))
      evaluation.evaluate(metrics) must be(Some(true))
    }

    "evaluate <=" in {
      val evaluation = new MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueLessEqualThan(BigDecimal(10)))), "")
      val metrics = new MetricsStore(evaluation)
      metrics.append(source, BigDecimal(5))
      evaluation.evaluate(metrics) must be(Some(true))
      metrics.append(source, BigDecimal(10))
      evaluation.evaluate(metrics) must be(Some(true))
      metrics.append(source, BigDecimal(15))
      evaluation.evaluate(metrics) must be(Some(false))
    }

    "evaluate >=" in {
      val evaluation = new MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueGreaterEqualThan(BigDecimal(10)))), "")
      val metrics = new MetricsStore(evaluation)
      metrics.append(source, BigDecimal(5))
      evaluation.evaluate(metrics) must be(Some(false))
      metrics.append(source, BigDecimal(10))
      evaluation.evaluate(metrics) must be(Some(true))
      metrics.append(source, BigDecimal(15))
      evaluation.evaluate(metrics) must be(Some(true))
    }
  }
}

class MetricWindowSpec extends WordSpec with MustMatchers {

  "MetricsWindow" must {

    "maintain invariants for an empty window" in {
      val window = new MetricWindow(5)
      window.headOption must be === None
      window.get(0) must be === None
      evaluating { window.head } must produce[NoSuchElementException]
      evaluating { window(0) } must produce[NoSuchElementException]
      evaluating { window.get(5) } must produce[IndexOutOfBoundsException]
      evaluating { window(5) } must produce[IndexOutOfBoundsException]
    }

    "append multiple elements into the window" in {
      val window = new MetricWindow(5)
      window.append(BigDecimal(1L))
      window.append(BigDecimal(2L))
      window.append(BigDecimal(3L))
      window.append(BigDecimal(4L))
      window.append(BigDecimal(5L))
      window(0) must be === BigDecimal(5L)
      window(1) must be === BigDecimal(4L)
      window(2) must be === BigDecimal(3L)
      window(3) must be === BigDecimal(2L)
      window(4) must be === BigDecimal(1L)
      window.append(BigDecimal(6L))
      window.append(BigDecimal(7L))
      window.append(BigDecimal(8L))
      window(0) must be === BigDecimal(8L)
      window(1) must be === BigDecimal(7L)
      window(2) must be === BigDecimal(6L)
      window(3) must be === BigDecimal(5L)
      window(4) must be === BigDecimal(4L)
    }

    "get the head element" in {
      val window = new MetricWindow(5)
      window.append(BigDecimal(3L))
      window.append(BigDecimal(2L))
      window.append(BigDecimal(1L))
      window.head must be === BigDecimal(1L)
      window.headOption must be === Some(BigDecimal(1L))
      window.get(0) must be === Some(BigDecimal(1L))
      window(0) must be === BigDecimal(1L)
    }

    "retrieve elements by index" in {
      val window = new MetricWindow(5)
      window.append(BigDecimal(3L))
      window.append(BigDecimal(2L))
      window.append(BigDecimal(1L))
      window.get(0) must be === Some(BigDecimal(1L))
      window.get(1) must be === Some(BigDecimal(2L))
      window.get(2) must be === Some(BigDecimal(3L))
      window.get(3) must be === None
      window.get(4) must be === None
      evaluating { window.get(5) } must produce[IndexOutOfBoundsException]
      window(0) must be === BigDecimal(1L)
      window(1) must be === BigDecimal(2L)
      window(2) must be === BigDecimal(3L)
      evaluating { window(3) } must produce[NoSuchElementException]
      evaluating { window(4) } must produce[NoSuchElementException]
      evaluating { window(5) } must produce[IndexOutOfBoundsException]
    }

    "fold elements" in {
      val window = new MetricWindow(5)
      window.append(BigDecimal(3L))
      window.append(BigDecimal(2L))
      window.append(BigDecimal(1L))
      window.foldLeft(0L) { case (v,sum) => v.toLong + sum } must be === 6L
    }

    "resize with new size greater than old size" in {
      val window = new MetricWindow(3)
      window.append(BigDecimal(3L))
      window.append(BigDecimal(2L))
      window.append(BigDecimal(1L))
      window.resize(5)
      window.get(0) must be === Some(BigDecimal(1L))
      window.get(1) must be === Some(BigDecimal(2L))
      window.get(2) must be === Some(BigDecimal(3L))
      window.get(3) must be === None
      window.get(4) must be === None
    }

    "resize with new size smaller than old size" in {
      val window = new MetricWindow(5)
      window.append(BigDecimal(5L))
      window.append(BigDecimal(4L))
      window.append(BigDecimal(3L))
      window.append(BigDecimal(2L))
      window.append(BigDecimal(1L))
      window.resize(3)
      window.get(0) must be === Some(BigDecimal(1L))
      window.get(1) must be === Some(BigDecimal(2L))
      window.get(2) must be === Some(BigDecimal(3L))
      evaluating { window.get(3) } must produce[IndexOutOfBoundsException]
      evaluating { window.get(4) } must produce[IndexOutOfBoundsException]
    }
  }
}