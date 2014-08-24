package io.mandelbrot.core.system

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import scala.collection.mutable

class MetricsEvaluationSpec extends WordSpec with MustMatchers {

  "MetricsEvaluation" must {

    "evaluate ==" in {
      val evaluation = ValueEquals(GaugeSource("foo"), MetricValue(10))
      evaluation.evaluate(mutable.HashMap(GaugeSource("foo") -> MetricValue(10))) must be(true)
      evaluation.evaluate(mutable.HashMap(GaugeSource("foo") -> MetricValue(11))) must be(false)
    }

    "evaluate !=" in {
      val evaluation = ValueNotEquals(GaugeSource("foo"), MetricValue(10))
      evaluation.evaluate(mutable.HashMap(GaugeSource("foo") -> MetricValue(10))) must be(false)
      evaluation.evaluate(mutable.HashMap(GaugeSource("foo") -> MetricValue(11))) must be(true)
    }

    "evaluate <" in {
      val evaluation = ValueLessThan(GaugeSource("foo"), MetricValue(10))
      evaluation.evaluate(mutable.HashMap(GaugeSource("foo") -> MetricValue(5))) must be(true)
      evaluation.evaluate(mutable.HashMap(GaugeSource("foo") -> MetricValue(10))) must be(false)
      evaluation.evaluate(mutable.HashMap(GaugeSource("foo") -> MetricValue(15))) must be(false)
    }

    "evaluate >" in {
      val evaluation = ValueGreaterThan(GaugeSource("foo"), MetricValue(10))
      evaluation.evaluate(mutable.HashMap(GaugeSource("foo") -> MetricValue(15))) must be(true)
      evaluation.evaluate(mutable.HashMap(GaugeSource("foo") -> MetricValue(10))) must be(false)
      evaluation.evaluate(mutable.HashMap(GaugeSource("foo") -> MetricValue(5))) must be(false)
    }
  }
}
