package io.mandelbrot.core.system

import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers

import io.mandelbrot.core.model._

class ProbeMatcherSpec extends WordSpec with ShouldMatchers {

  "ProbeMatcher" should {

    "match '*'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*")
      matcher.matches(ProbeRef("fqdn:localhost/load")) should be(true)
    }

    "match '*:*'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:*")
      matcher.matches(ProbeRef("fqdn:localhost/load")) should be(true)
    }

    "match when scheme matches" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("fqdn:*")
      matcher.matches(ProbeRef("fqdn:localhost/load")) should be(true)
    }

    "not match when scheme doesn't match" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("ipv4:*")
      matcher.matches(ProbeRef("fqdn:localhost/load")) should be(false)
    }

    "match when location matches" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:example.com")
      matcher.matches(ProbeRef("fqdn:example.com/load")) should be(true)
    }

    "not match when location doesn't match" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:example.com")
      matcher.matches(ProbeRef("fqdn:localhost/load")) should be(false)
    }

    "match when path matches" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:*/load")
      matcher.matches(ProbeRef("fqdn:example.com/load")) should be(true)
    }

    "not match when path doesn't match" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:*/load")
      matcher.matches(ProbeRef("fqdn:localhost/cpu")) should be(false)
    }

    "match when path wildcard prefix matches" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:*/*ad")
      println(matcher)
      matcher.matches(ProbeRef("fqdn:example.com/load")) should be(true)
    }

    "not match when path wildcard prefix doesn't match" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:*/*ad")
      matcher.matches(ProbeRef("fqdn:localhost/cpu")) should be(false)
    }

    "match when path wildcard matches" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:*/l*d")
      println(matcher)
      matcher.matches(ProbeRef("fqdn:example.com/load")) should be(true)
    }

    "not match when path wildcard doesn't match" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:*/l*d")
      matcher.matches(ProbeRef("fqdn:localhost/cpu")) should be(false)
    }

    "match when path wildcard suffix matches" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:*/lo*")
      println(matcher)
      matcher.matches(ProbeRef("fqdn:example.com/load")) should be(true)
    }

    "not match when path wildcard suffix doesn't match" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:*/lo*")
      matcher.matches(ProbeRef("fqdn:localhost/cpu")) should be(false)
    }

  }
}
