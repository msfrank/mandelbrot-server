package io.mandelbrot.core.system

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import scala.collection.mutable

class MetricsEvaluationSpec extends WordSpec with MustMatchers {

  "MetricsEvaluation" must {

    "evaluate ==" in {
      val evaluation = ValueEquals(MetricSource(Vector.empty, "foo"), MetricValue(10))
      val metrics = new MetricsStore(evaluation)
      metrics.push(MetricSource(Vector.empty, "foo"), MetricValue(10))
      evaluation.evaluate(metrics) must be(true)
      metrics.push(MetricSource(Vector.empty, "foo"), MetricValue(11))
      evaluation.evaluate(metrics) must be(false)
    }

    "evaluate !=" in {
      val evaluation = ValueNotEquals(MetricSource(Vector.empty, "foo"), MetricValue(10))
      val metrics = new MetricsStore(evaluation)
      metrics.push(MetricSource(Vector.empty, "foo"), MetricValue(10))
      evaluation.evaluate(metrics) must be(false)
      metrics.push(MetricSource(Vector.empty, "foo"), MetricValue(11))
      evaluation.evaluate(metrics) must be(true)
    }

    "evaluate <" in {
      val evaluation = ValueLessThan(MetricSource(Vector.empty, "foo"), MetricValue(10))
      val metrics = new MetricsStore(evaluation)
      metrics.push(MetricSource(Vector.empty, "foo"), MetricValue(5))
      evaluation.evaluate(metrics) must be(true)
      metrics.push(MetricSource(Vector.empty, "foo"), MetricValue(10))
      evaluation.evaluate(metrics) must be(false)
      metrics.push(MetricSource(Vector.empty, "foo"), MetricValue(15))
      evaluation.evaluate(metrics) must be(false)
    }

    "evaluate >" in {
      val evaluation = ValueGreaterThan(MetricSource(Vector.empty, "foo"), MetricValue(10))
      val metrics = new MetricsStore(evaluation)
      metrics.push(MetricSource(Vector.empty, "foo"), MetricValue(5))
      evaluation.evaluate(metrics) must be(false)
      metrics.push(MetricSource(Vector.empty, "foo"), MetricValue(10))
      evaluation.evaluate(metrics) must be(false)
      metrics.push(MetricSource(Vector.empty, "foo"), MetricValue(15))
      evaluation.evaluate(metrics) must be(true)
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

    "push multiple elements into the window" in {
      val window = new MetricWindow(5)
      window.push(MetricValue(1L))
      window.push(MetricValue(2L))
      window.push(MetricValue(3L))
      window.push(MetricValue(4L))
      window.push(MetricValue(5L))
      window(0) must be === MetricValue(5L)
      window(1) must be === MetricValue(4L)
      window(2) must be === MetricValue(3L)
      window(3) must be === MetricValue(2L)
      window(4) must be === MetricValue(1L)
      window.push(MetricValue(6L))
      window.push(MetricValue(7L))
      window.push(MetricValue(8L))
      window(0) must be === MetricValue(8L)
      window(1) must be === MetricValue(7L)
      window(2) must be === MetricValue(6L)
      window(3) must be === MetricValue(5L)
      window(4) must be === MetricValue(4L)
    }

    "get the head element" in {
      val window = new MetricWindow(5)
      window.push(MetricValue(3L))
      window.push(MetricValue(2L))
      window.push(MetricValue(1L))
      window.head must be === MetricValue(1L)
      window.headOption must be === Some(MetricValue(1L))
      window.get(0) must be === Some(MetricValue(1L))
      window(0) must be === MetricValue(1L)
    }

    "retrieve elements by index" in {
      val window = new MetricWindow(5)
      window.push(MetricValue(3L))
      window.push(MetricValue(2L))
      window.push(MetricValue(1L))
      window.get(0) must be === Some(MetricValue(1L))
      window.get(1) must be === Some(MetricValue(2L))
      window.get(2) must be === Some(MetricValue(3L))
      window.get(3) must be === None
      window.get(4) must be === None
      evaluating { window.get(5) } must produce[IndexOutOfBoundsException]
      window(0) must be === MetricValue(1L)
      window(1) must be === MetricValue(2L)
      window(2) must be === MetricValue(3L)
      evaluating { window(3) } must produce[NoSuchElementException]
      evaluating { window(4) } must produce[NoSuchElementException]
      evaluating { window(5) } must produce[IndexOutOfBoundsException]
    }

    "fold elements" in {
      val window = new MetricWindow(5)
      window.push(MetricValue(3L))
      window.push(MetricValue(2L))
      window.push(MetricValue(1L))
      window.foldLeft(0L) { case (v,sum) => v.toLong + sum } must be === 6L
    }

    "resize with new size greater than old size" in {
      val window = new MetricWindow(3)
      window.push(MetricValue(3L))
      window.push(MetricValue(2L))
      window.push(MetricValue(1L))
      window.resize(5)
      window.get(0) must be === Some(MetricValue(1L))
      window.get(1) must be === Some(MetricValue(2L))
      window.get(2) must be === Some(MetricValue(3L))
      window.get(3) must be === None
      window.get(4) must be === None
    }

    "resize with new size smaller than old size" in {
      val window = new MetricWindow(5)
      window.push(MetricValue(5L))
      window.push(MetricValue(4L))
      window.push(MetricValue(3L))
      window.push(MetricValue(2L))
      window.push(MetricValue(1L))
      window.resize(3)
      window.get(0) must be === Some(MetricValue(1L))
      window.get(1) must be === Some(MetricValue(2L))
      window.get(2) must be === Some(MetricValue(3L))
      evaluating { window.get(3) } must produce[IndexOutOfBoundsException]
      evaluating { window.get(4) } must produce[IndexOutOfBoundsException]
    }
  }
}