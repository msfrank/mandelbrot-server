package io.mandelbrot.core.metrics

import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers
import scala.math.BigDecimal

import io.mandelbrot.core.model._

class TimeseriesWindowSpec extends WordSpec with ShouldMatchers {

  "MetricsWindow" should {

    val timestamp = DateTime.now(DateTimeZone.UTC)
    val status1 = ScalarMapObservation(timestamp, Map("value" -> BigDecimal(1)))
    val status2 = ScalarMapObservation(timestamp.plus(1), Map("value" -> BigDecimal(2)))
    val status3 = ScalarMapObservation(timestamp.plus(2), Map("value" -> BigDecimal(3)))
    val status4 = ScalarMapObservation(timestamp.plus(3), Map("value" -> BigDecimal(4)))
    val status5 = ScalarMapObservation(timestamp.plus(4), Map("value" -> BigDecimal(5)))
    val status6 = ScalarMapObservation(timestamp.plus(5), Map("value" -> BigDecimal(6)))
    val status7 = ScalarMapObservation(timestamp.plus(6), Map("value" -> BigDecimal(7)))
    val status8 = ScalarMapObservation(timestamp.plus(7), Map("value" -> BigDecimal(8)))
    val status9 = ScalarMapObservation(timestamp.plus(8), Map("value" -> BigDecimal(9)))

    "maintain invariants for an empty window" in {
      val window = new TimeseriesWindow(5)
      window.headOption shouldEqual None
      window.get(0) shouldEqual None
      a [NoSuchElementException] should be thrownBy { window.head }
      a [NoSuchElementException] should be thrownBy { window(0) }
      an [IndexOutOfBoundsException] should be thrownBy { window.get(5) }
      an [IndexOutOfBoundsException] should be thrownBy { window(5) }
    }

    "append multiple elements into the window" in {
      val window = new TimeseriesWindow(5)
      window.append(status1)
      window.append(status2)
      window.append(status3)
      window.append(status4)
      window.append(status5)
      window(0) shouldEqual status5
      window(1) shouldEqual status4
      window(2) shouldEqual status3
      window(3) shouldEqual status2
      window(4) shouldEqual status1
      window.append(status6)
      window.append(status7)
      window.append(status8)
      window(0) shouldEqual status8
      window(1) shouldEqual status7
      window(2) shouldEqual status6
      window(3) shouldEqual status5
      window(4) shouldEqual status4
    }

    "get the head element" in {
      val window = new TimeseriesWindow(5)
      window.append(status3)
      window.append(status2)
      window.append(status1)
      window.head shouldEqual status1
      window.headOption shouldEqual Some(status1)
      window.get(0) shouldEqual Some(status1)
      window(0) shouldEqual status1
    }

    "retrieve elements by index" in {
      val window = new TimeseriesWindow(5)
      window.append(status3)
      window.append(status2)
      window.append(status1)
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
      val window = new TimeseriesWindow(5)
      window.append(status3)
      window.append(status2)
      window.append(status1)
      window.foldLeft(0L) { case (v: ScalarMapObservation,sum) => v.metrics("value").toLong + sum } shouldEqual 6L
    }

    "resize with new size greater than old size" in {
      val window = new TimeseriesWindow(3)
      window.append(status3)
      window.append(status2)
      window.append(status1)
      window.resize(5)
      window.get(0) shouldEqual Some(status1)
      window.get(1) shouldEqual Some(status2)
      window.get(2) shouldEqual Some(status3)
      window.get(3) shouldEqual None
      window.get(4) shouldEqual None
    }

    "resize with new size smaller than old size" in {
      val window = new TimeseriesWindow(5)
      window.append(status5)
      window.append(status4)
      window.append(status3)
      window.append(status2)
      window.append(status1)
      window.resize(3)
      window.get(0) shouldEqual Some(status1)
      window.get(1) shouldEqual Some(status2)
      window.get(2) shouldEqual Some(status3)
      an [IndexOutOfBoundsException] should be thrownBy { window.get(3) }
      an [IndexOutOfBoundsException] should be thrownBy { window.get(4) }
    }
  }
}