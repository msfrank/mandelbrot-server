package io.mandelbrot.core.system

import org.scalatest.Inside._
import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers

import io.mandelbrot.core.model._

class ProbeMatcherParserSpec extends WordSpec with ShouldMatchers {

  "ProbeMatcherParser" should {

    "parse '*'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*")
      println(matcher)
      matcher.getClass shouldEqual MatchesAll.getClass
    }

    "parse '*:*'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:*")
      println(matcher)
      inside(matcher) {
        case ProbeMatcher(scheme, location, path) =>
          scheme shouldEqual Some(MatchAny)
          location shouldEqual Some(MatchAny)
          path shouldEqual None
      }
    }

    "parse '*:example.com'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:example.com")
      println(matcher)
      inside(matcher) {
        case ProbeMatcher(Some(MatchAny), Some(location: MatchExact), None) =>
          location.string shouldEqual "example.com"
      }
    }

    "parse 'fqdn:*.com'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("fqdn:*.com")
      println(matcher)
      inside(matcher) {
        case ProbeMatcher(Some(scheme: MatchExact), Some(location: MatchGlob), None) =>
          scheme.string shouldEqual "fqdn"
          location.regex.toString shouldEqual """.*\Q.com\E"""
      }
    }

    "parse 'fqdn:example.com'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("fqdn:example.com")
      println(matcher)
      inside(matcher) {
        case ProbeMatcher(Some(scheme: MatchExact), Some(location: MatchExact), None) =>
          scheme.string shouldEqual "fqdn"
          location.string shouldEqual "example.com"
      }
    }

    "parse 'fqdn:example.com/foo'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("fqdn:example.com/foo")
      println(matcher)
      inside(matcher) {
        case ProbeMatcher(Some(scheme: MatchExact), Some(location: MatchExact), Some(path: PathMatcher)) =>
          scheme.string shouldEqual "fqdn"
          location.string shouldEqual "example.com"
          path.segments shouldEqual Vector(MatchExact("foo"))
      }
    }

    "parse 'fqdn:example.com/foo/bar/baz'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("fqdn:example.com/foo/bar/baz")
      println(matcher)
      inside(matcher) {
        case ProbeMatcher(Some(scheme: MatchExact), Some(location: MatchExact), Some(path: PathMatcher)) =>
          scheme.string shouldEqual "fqdn"
          location.string shouldEqual "example.com"
          path.segments shouldEqual Vector(MatchExact("foo"), MatchExact("bar"), MatchExact("baz"))
      }
    }

    "parse 'fqdn:example.com/foo/*'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("fqdn:example.com/foo/*")
      println(matcher)
      inside(matcher) {
        case ProbeMatcher(Some(scheme: MatchExact), Some(location: MatchExact), Some(path: PathMatcher)) =>
          scheme.string shouldEqual "fqdn"
          location.string shouldEqual "example.com"
          path.segments shouldEqual Vector(MatchExact("foo"), MatchAny)
      }
    }

  }
}
