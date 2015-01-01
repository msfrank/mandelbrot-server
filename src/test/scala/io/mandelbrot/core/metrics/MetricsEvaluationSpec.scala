package io.mandelbrot.core.metrics

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

import scala.math.BigDecimal

class MetricsEvaluationSpec extends WordSpec with ShouldMatchers {

  "MetricsEvaluation" should {

    val source = MetricSource(Vector.empty, "foo")

    "evaluate ==" in {
      val evaluation = new MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueEquals(BigDecimal(10)))), "")
      val metrics = new MetricsStore(evaluation)
      metrics.append(source, BigDecimal(10))
      evaluation.evaluate(metrics) should be(Some(true))
      metrics.append(source, BigDecimal(11))
      evaluation.evaluate(metrics) should be(Some(false))
    }

    "evaluate !=" in {
      val evaluation = new MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueNotEquals(BigDecimal(10)))), "")
      val metrics = new MetricsStore(evaluation)
      metrics.append(source, BigDecimal(10))
      evaluation.evaluate(metrics) should be(Some(false))
      metrics.append(source, BigDecimal(11))
      evaluation.evaluate(metrics) should be(Some(true))
    }

    "evaluate <" in {
      val evaluation = new MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueLessThan(BigDecimal(10)))), "")
      val metrics = new MetricsStore(evaluation)
      metrics.append(source, BigDecimal(5))
      evaluation.evaluate(metrics) should be(Some(true))
      metrics.append(source, BigDecimal(10))
      evaluation.evaluate(metrics) should be(Some(false))
      metrics.append(source, BigDecimal(15))
      evaluation.evaluate(metrics) should be(Some(false))
    }

    "evaluate >" in {
      val evaluation = new MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueGreaterThan(BigDecimal(10)))), "")
      val metrics = new MetricsStore(evaluation)
      metrics.append(source, BigDecimal(5))
      evaluation.evaluate(metrics) should be(Some(false))
      metrics.append(source, BigDecimal(10))
      evaluation.evaluate(metrics) should be(Some(false))
      metrics.append(source, BigDecimal(15))
      evaluation.evaluate(metrics) should be(Some(true))
    }

    "evaluate <=" in {
      val evaluation = new MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueLessEqualThan(BigDecimal(10)))), "")
      val metrics = new MetricsStore(evaluation)
      metrics.append(source, BigDecimal(5))
      evaluation.evaluate(metrics) should be(Some(true))
      metrics.append(source, BigDecimal(10))
      evaluation.evaluate(metrics) should be(Some(true))
      metrics.append(source, BigDecimal(15))
      evaluation.evaluate(metrics) should be(Some(false))
    }

    "evaluate >=" in {
      val evaluation = new MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueGreaterEqualThan(BigDecimal(10)))), "")
      val metrics = new MetricsStore(evaluation)
      metrics.append(source, BigDecimal(5))
      evaluation.evaluate(metrics) should be(Some(false))
      metrics.append(source, BigDecimal(10))
      evaluation.evaluate(metrics) should be(Some(true))
      metrics.append(source, BigDecimal(15))
      evaluation.evaluate(metrics) should be(Some(true))
    }
  }
}

class MetricWindowSpec extends WordSpec with ShouldMatchers {

  "MetricsWindow" should {

    "maintain invariants for an empty window" in {
      val window = new MetricWindow(5)
      window.headOption shouldEqual None
      window.get(0) shouldEqual None
      evaluating { window.head } should produce[NoSuchElementException]
      evaluating { window(0) } should produce[NoSuchElementException]
      evaluating { window.get(5) } should produce[IndexOutOfBoundsException]
      evaluating { window(5) } should produce[IndexOutOfBoundsException]
    }

    "append multiple elements into the window" in {
      val window = new MetricWindow(5)
      window.append(BigDecimal(1L))
      window.append(BigDecimal(2L))
      window.append(BigDecimal(3L))
      window.append(BigDecimal(4L))
      window.append(BigDecimal(5L))
      window(0) shouldEqual BigDecimal(5L)
      window(1) shouldEqual BigDecimal(4L)
      window(2) shouldEqual BigDecimal(3L)
      window(3) shouldEqual BigDecimal(2L)
      window(4) shouldEqual BigDecimal(1L)
      window.append(BigDecimal(6L))
      window.append(BigDecimal(7L))
      window.append(BigDecimal(8L))
      window(0) shouldEqual BigDecimal(8L)
      window(1) shouldEqual BigDecimal(7L)
      window(2) shouldEqual BigDecimal(6L)
      window(3) shouldEqual BigDecimal(5L)
      window(4) shouldEqual BigDecimal(4L)
    }

    "get the head element" in {
      val window = new MetricWindow(5)
      window.append(BigDecimal(3L))
      window.append(BigDecimal(2L))
      window.append(BigDecimal(1L))
      window.head shouldEqual BigDecimal(1L)
      window.headOption shouldEqual Some(BigDecimal(1L))
      window.get(0) shouldEqual Some(BigDecimal(1L))
      window(0) shouldEqual BigDecimal(1L)
    }

    "retrieve elements by index" in {
      val window = new MetricWindow(5)
      window.append(BigDecimal(3L))
      window.append(BigDecimal(2L))
      window.append(BigDecimal(1L))
      window.get(0) shouldEqual Some(BigDecimal(1L))
      window.get(1) shouldEqual Some(BigDecimal(2L))
      window.get(2) shouldEqual Some(BigDecimal(3L))
      window.get(3) shouldEqual None
      window.get(4) shouldEqual None
      evaluating { window.get(5) } should produce[IndexOutOfBoundsException]
      window(0) shouldEqual BigDecimal(1L)
      window(1) shouldEqual BigDecimal(2L)
      window(2) shouldEqual BigDecimal(3L)
      evaluating { window(3) } should produce[NoSuchElementException]
      evaluating { window(4) } should produce[NoSuchElementException]
      evaluating { window(5) } should produce[IndexOutOfBoundsException]
    }

    "fold elements" in {
      val window = new MetricWindow(5)
      window.append(BigDecimal(3L))
      window.append(BigDecimal(2L))
      window.append(BigDecimal(1L))
      window.foldLeft(0L) { case (v,sum) => v.toLong + sum } shouldEqual 6L
    }

    "resize with new size greater than old size" in {
      val window = new MetricWindow(3)
      window.append(BigDecimal(3L))
      window.append(BigDecimal(2L))
      window.append(BigDecimal(1L))
      window.resize(5)
      window.get(0) shouldEqual Some(BigDecimal(1L))
      window.get(1) shouldEqual Some(BigDecimal(2L))
      window.get(2) shouldEqual Some(BigDecimal(3L))
      window.get(3) shouldEqual None
      window.get(4) shouldEqual None
    }

    "resize with new size smaller than old size" in {
      val window = new MetricWindow(5)
      window.append(BigDecimal(5L))
      window.append(BigDecimal(4L))
      window.append(BigDecimal(3L))
      window.append(BigDecimal(2L))
      window.append(BigDecimal(1L))
      window.resize(3)
      window.get(0) shouldEqual Some(BigDecimal(1L))
      window.get(1) shouldEqual Some(BigDecimal(2L))
      window.get(2) shouldEqual Some(BigDecimal(3L))
      evaluating { window.get(3) } should produce[IndexOutOfBoundsException]
      evaluating { window.get(4) } should produce[IndexOutOfBoundsException]
    }
  }
}