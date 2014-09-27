package io.mandelbrot.core.system

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import scala.math.BigDecimal

class MetricsEvaluationParserSpec extends WordSpec with MustMatchers {

  "MetricsEvaluationParser" must {

    val source = MetricSource(Vector.empty, "foo")

    "parse 'when foo == 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo == 0")
      println(evaluation)
      evaluation must be === MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueEquals(BigDecimal(0)))))
    }

    "parse 'when foo != 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo != 0")
      println(evaluation)
      evaluation must be === MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueNotEquals(BigDecimal(0)))))
    }

    "parse 'when foo < 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo < 0")
      println(evaluation)
      evaluation must be === MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueLessThan(BigDecimal(0)))))
    }

    "parse 'when foo > 0'" in {
      val evaluation = new MetricsEvaluationParser().parseMetricsEvaluation("when foo > 0")
      println(evaluation)
      evaluation must be === MetricsEvaluation(EvaluateSource(source, HeadFunction(ValueGreaterThan(BigDecimal(0)))))
    }
  }
}
