package io.mandelbrot.core.system

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

class MetricsEvaluationParserSpec extends WordSpec with MustMatchers {

  "MetricsEvaluationParser" must {

    "parse 'when foo == 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo == 0")
      println(evaluation)
      evaluation must be === ValueEquals(GaugeSource("foo"), MetricValue(0))
    }

    "parse 'when foo != 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo != 0")
      println(evaluation)
      evaluation must be === ValueNotEquals(GaugeSource("foo"), MetricValue(0))
    }

    "parse 'when foo < 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo < 0")
      println(evaluation)
      evaluation must be === ValueLessThan(GaugeSource("foo"), MetricValue(0))
    }

    "parse 'when foo > 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo > 0")
      println(evaluation)
      evaluation must be === ValueGreaterThan(GaugeSource("foo"), MetricValue(0))
    }
  }
}
