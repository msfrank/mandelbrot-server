package io.mandelbrot.core.model

import org.scalatest.{ShouldMatchers, WordSpec}

class GenerationLsnSpec extends WordSpec with ShouldMatchers {

  "A GenerationLsn" should {
    "order correctly by generation, then by lsn" in {
      GenerationLsn(1L, 1L) compare GenerationLsn(1L, 1L) shouldEqual 0
      GenerationLsn(1L, 1L) compare GenerationLsn(2L, 2L) shouldEqual -1
      GenerationLsn(3L, 3L) compare GenerationLsn(2L, 2L) shouldEqual 1
      GenerationLsn(1L, 4L) compare GenerationLsn(2L, 3L) shouldEqual -1
      GenerationLsn(3L, 1L) compare GenerationLsn(2L, 4L) shouldEqual 1
    }
  }
}
