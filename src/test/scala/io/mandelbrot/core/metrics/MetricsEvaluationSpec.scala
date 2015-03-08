package io.mandelbrot.core.metrics

import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers

import scala.math.BigDecimal

class MetricsEvaluationSpec extends WordSpec with ShouldMatchers {

  "MetricsEvaluation" should {

    val source = MetricSource(Vector.empty, "foo")

    "evaluate ==" in {
      val evaluation = new MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueEquals(BigDecimal(10)))), "")
      val metrics = new MetricsStore(evaluation)
      metrics.append(source, BigDecimal(10))
      evaluation.evaluate(metrics) shouldEqual Some(true)
      metrics.append(source, BigDecimal(11))
      evaluation.evaluate(metrics) shouldEqual Some(false)
    }

    "evaluate !=" in {
      val evaluation = new MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueNotEquals(BigDecimal(10)))), "")
      val metrics = new MetricsStore(evaluation)
      metrics.append(source, BigDecimal(10))
      evaluation.evaluate(metrics) shouldEqual Some(false)
      metrics.append(source, BigDecimal(11))
      evaluation.evaluate(metrics) shouldEqual Some(true)
    }

    "evaluate <" in {
      val evaluation = new MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueLessThan(BigDecimal(10)))), "")
      val metrics = new MetricsStore(evaluation)
      metrics.append(source, BigDecimal(5))
      evaluation.evaluate(metrics) shouldEqual Some(true)
      metrics.append(source, BigDecimal(10))
      evaluation.evaluate(metrics) shouldEqual Some(false)
      metrics.append(source, BigDecimal(15))
      evaluation.evaluate(metrics) shouldEqual Some(false)
    }

    "evaluate >" in {
      val evaluation = new MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueGreaterThan(BigDecimal(10)))), "")
      val metrics = new MetricsStore(evaluation)
      metrics.append(source, BigDecimal(5))
      evaluation.evaluate(metrics) shouldEqual Some(false)
      metrics.append(source, BigDecimal(10))
      evaluation.evaluate(metrics) shouldEqual Some(false)
      metrics.append(source, BigDecimal(15))
      evaluation.evaluate(metrics) shouldEqual Some(true)
    }

    "evaluate <=" in {
      val evaluation = new MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueLessEqualThan(BigDecimal(10)))), "")
      val metrics = new MetricsStore(evaluation)
      metrics.append(source, BigDecimal(5))
      evaluation.evaluate(metrics) shouldEqual Some(true)
      metrics.append(source, BigDecimal(10))
      evaluation.evaluate(metrics) shouldEqual Some(true)
      metrics.append(source, BigDecimal(15))
      evaluation.evaluate(metrics) shouldEqual Some(false)
    }

    "evaluate >=" in {
      val evaluation = new MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueGreaterEqualThan(BigDecimal(10)))), "")
      val metrics = new MetricsStore(evaluation)
      metrics.append(source, BigDecimal(5))
      evaluation.evaluate(metrics) shouldEqual Some(false)
      metrics.append(source, BigDecimal(10))
      evaluation.evaluate(metrics) shouldEqual Some(true)
      metrics.append(source, BigDecimal(15))
      evaluation.evaluate(metrics) shouldEqual Some(true)
    }
  }
}

class MetricWindowSpec extends WordSpec with ShouldMatchers {

  "MetricsWindow" should {

    "maintain invariants for an empty window" in {
      val window = new MetricWindow(5)
      window.headOption shouldEqual None
      window.get(0) shouldEqual None
      a [NoSuchElementException] should be thrownBy { window.head }
      a [NoSuchElementException] should be thrownBy { window(0) }
      an [IndexOutOfBoundsException] should be thrownBy { window.get(5) }
      an [IndexOutOfBoundsException] should be thrownBy { window(5) }
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
      an [IndexOutOfBoundsException] should be thrownBy { window.get(5) }
      window(0) shouldEqual BigDecimal(1L)
      window(1) shouldEqual BigDecimal(2L)
      window(2) shouldEqual BigDecimal(3L)
      a [NoSuchElementException] should be thrownBy { window(3) }
      a [NoSuchElementException] should be thrownBy { window(4) }
      an [IndexOutOfBoundsException] should be thrownBy { window(5) }
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
      an [IndexOutOfBoundsException] should be thrownBy { window.get(3) }
      an [IndexOutOfBoundsException] should be thrownBy { window.get(4) }
    }
  }
}