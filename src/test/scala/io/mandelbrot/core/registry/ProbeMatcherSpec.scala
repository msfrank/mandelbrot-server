package io.mandelbrot.core.registry

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

class ProbeMatcherSpec extends WordSpec with MustMatchers {

  "ProbeMatcher" must {

    "match '*'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*")
      matcher.matches(ProbeRef("fqdn:localhost/load")) must be(true)
    }

    "match '*:*'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:*")
      matcher.matches(ProbeRef("fqdn:localhost/load")) must be(true)
    }

    "match when scheme matches" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("fqdn:*")
      matcher.matches(ProbeRef("fqdn:localhost/load")) must be(true)
    }

    "not match when scheme doesn't match" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("ipv4:*")
      matcher.matches(ProbeRef("fqdn:localhost/load")) must be(false)
    }

    "match when location matches" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:example.com")
      matcher.matches(ProbeRef("fqdn:example.com/load")) must be(true)
    }

    "not match when location doesn't match" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:example.com")
      matcher.matches(ProbeRef("fqdn:localhost/load")) must be(false)
    }

    "match when path matches" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:*/load")
      matcher.matches(ProbeRef("fqdn:example.com/load")) must be(true)
    }

    "not match when path doesn't match" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:*/load")
      matcher.matches(ProbeRef("fqdn:localhost/cpu")) must be(false)
    }
  }
}
