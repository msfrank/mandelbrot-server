package io.mandelbrot.core.check

import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers

import io.mandelbrot.core.model._
import io.mandelbrot.core.parser.CheckMatcherParser

class CheckMatcherSpec extends WordSpec with ShouldMatchers {

  "CheckMatcher" should {

    "match '*'" in {
      val matcher = CheckMatcherParser.parseCheckMatcher("*")
      matcher.matches(CheckId("check.local.foo")) shouldEqual true
    }

    "match a literal path" in {
      val matcher = CheckMatcherParser.parseCheckMatcher("load")
      matcher.matches(CheckId("load")) shouldEqual true
    }

    "not match when literal path doesn't match" in {
      val matcher = CheckMatcherParser.parseCheckMatcher("load")
      matcher.matches(CheckId("cpu")) shouldEqual false
    }

    "match when path wildcard prefix matches" in {
      val matcher = CheckMatcherParser.parseCheckMatcher("*ad")
      matcher.matches(CheckId("load")) shouldEqual true
    }

    "not match when path wildcard prefix doesn't match" in {
      val matcher = CheckMatcherParser.parseCheckMatcher("*ad")
      matcher.matches(CheckId("cpu")) shouldEqual false
    }

    "match when path wildcard matches" in {
      val matcher = CheckMatcherParser.parseCheckMatcher("l*d")
      matcher.matches(CheckId("load")) shouldEqual true
    }

    "not match when path wildcard doesn't match" in {
      val matcher = CheckMatcherParser.parseCheckMatcher("l*d")
      matcher.matches(CheckId("cpu")) shouldEqual false
    }

    "match when path wildcard suffix matches" in {
      val matcher = CheckMatcherParser.parseCheckMatcher("lo*")
      matcher.matches(CheckId("load")) shouldEqual true
    }

    "not match when path wildcard suffix doesn't match" in {
      val matcher = CheckMatcherParser.parseCheckMatcher("lo*")
      matcher.matches(CheckId("cpu")) shouldEqual false
    }

  }
}
