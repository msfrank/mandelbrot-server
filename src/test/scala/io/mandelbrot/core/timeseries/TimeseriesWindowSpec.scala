package io.mandelbrot.core.timeseries

import org.scalatest.{ShouldMatchers, WordSpec}
import scala.concurrent.duration._

import io.mandelbrot.core.model._

class TimeseriesWindowSpec extends WordSpec with ShouldMatchers {

  "MetricsWindow" should {

    val probeId = ProbeId("foo.source")
    val metricName = "foovalue"
    val dimension = Dimension("agent", "foo.agent")
    val statistic = MetricMinimum

    val timestamp = Timestamp()
    val status1 = ProbeMetrics(probeId, metricName, dimension, timestamp, Map(MetricMinimum -> 1))
    val status2 = ProbeMetrics(probeId, metricName, dimension, timestamp + 1.minute, Map(MetricMinimum -> 2))
    val status3 = ProbeMetrics(probeId, metricName, dimension, timestamp + 2.minute, Map(MetricMinimum -> 3))
    val status4 = ProbeMetrics(probeId, metricName, dimension, timestamp + 3.minute, Map(MetricMinimum -> 4))
    val status5 = ProbeMetrics(probeId, metricName, dimension, timestamp + 4.minute, Map(MetricMinimum -> 5))
    val status6 = ProbeMetrics(probeId, metricName, dimension, timestamp + 5.minute, Map(MetricMinimum -> 6))
    val status7 = ProbeMetrics(probeId, metricName, dimension, timestamp + 6.minute, Map(MetricMinimum -> 7))
    val status8 = ProbeMetrics(probeId, metricName, dimension, timestamp + 7.minute, Map(MetricMinimum -> 8))
    val status9 = ProbeMetrics(probeId, metricName, dimension, timestamp + 8.minute, Map(MetricMinimum -> 9))

    "maintain invariants for an empty window" in {
      val window = new TimeseriesWindow(initialSize = 5, initialInstant = timestamp.toMillis, PerMinute)
      window.headOption shouldEqual None
      window.get(0) shouldEqual None
      a [NoSuchElementException] should be thrownBy { window.head }
      a [NoSuchElementException] should be thrownBy { window(0) }
      an [IndexOutOfBoundsException] should be thrownBy { window.get(5) }
      an [IndexOutOfBoundsException] should be thrownBy { window(5) }
    }

    "append multiple elements into the window" in {
      val window = new TimeseriesWindow(initialSize = 5, initialInstant = timestamp.toMillis, PerMinute)
      window.put(status1.timestamp, status1)
      window.put(status2.timestamp, status2)
      window.put(status3.timestamp, status3)
      window.put(status4.timestamp, status4)
      window.put(status5.timestamp, status5)
      window(0) shouldEqual status5
      window(1) shouldEqual status4
      window(2) shouldEqual status3
      window(3) shouldEqual status2
      window(4) shouldEqual status1
      window.put(status6.timestamp, status6)
      window.put(status7.timestamp, status7)
      window.put(status8.timestamp, status8)
      window(0) shouldEqual status8
      window(1) shouldEqual status7
      window(2) shouldEqual status6
      window(3) shouldEqual status5
      window(4) shouldEqual status4
    }

    "get the head element" in {
      val window = new TimeseriesWindow(initialSize = 5, initialInstant = timestamp.toMillis, PerMinute)
      window.put(status3.timestamp, status3)
      window.put(status2.timestamp, status2)
      window.put(status1.timestamp, status1)
      window.head shouldEqual status1
      window.headOption shouldEqual Some(status1)
      window.get(0) shouldEqual Some(status1)
      window(0) shouldEqual status1
    }

    "retrieve elements by index" in {
      val window = new TimeseriesWindow(initialSize = 5, initialInstant = timestamp.toMillis, PerMinute)
      window.put(status3.timestamp, status3)
      window.put(status2.timestamp, status2)
      window.put(status1.timestamp, status1)
      window.get(0) shouldEqual Some(status1)
      window.get(1) shouldEqual Some(status2)
      window.get(2) shouldEqual Some(status3)
      window.get(3) shouldEqual None
      window.get(4) shouldEqual None
      an [IndexOutOfBoundsException] should be thrownBy { window.get(5) }
      window(0) shouldEqual status1
      window(1) shouldEqual status2
      window(2) shouldEqual status3
      a [NoSuchElementException] should be thrownBy { window(3) }
      a [NoSuchElementException] should be thrownBy { window(4) }
      an [IndexOutOfBoundsException] should be thrownBy { window(5) }
    }

    "fold elements" in {
      val window = new TimeseriesWindow(initialSize = 5, initialInstant = timestamp.toMillis, PerMinute)
      window.put(status3.timestamp, status3)
      window.put(status2.timestamp, status2)
      window.put(status1.timestamp, status1)
      window.foldLeft(0L) { case (v: ProbeMetrics,sum) => v.statistics(MetricMinimum).toLong + sum } shouldEqual 6L
    }
//
//    "resize with new size greater than old size" in {
//      val window = new TimeseriesWindow(initialSize = 3, initialInstant = timestamp.toMillis, PerMinute)
//      window.put(status3.timestamp, status3)
//      window.put(status2.timestamp, status2)
//      window.put(status1.timestamp, status1)
//      window.resize(5)
//      window.get(0) shouldEqual Some(status1)
//      window.get(1) shouldEqual Some(status2)
//      window.get(2) shouldEqual Some(status3)
//      window.get(3) shouldEqual None
//      window.get(4) shouldEqual None
//    }
//
//    "resize with new size smaller than old size" in {
//      val window = new TimeseriesWindow(initialSize = 5, initialInstant = timestamp.toMillis, PerMinute)
//      window.put(status5.timestamp, status5)
//      window.put(status4.timestamp, status4)
//      window.put(status3.timestamp, status3)
//      window.put(status2.timestamp, status2)
//      window.put(status1.timestamp, status1)
//      window.resize(3)
//      window.get(0) shouldEqual Some(status1)
//      window.get(1) shouldEqual Some(status2)
//      window.get(2) shouldEqual Some(status3)
//      an [IndexOutOfBoundsException] should be thrownBy { window.get(3) }
//      an [IndexOutOfBoundsException] should be thrownBy { window.get(4) }
//    }
  }
}
