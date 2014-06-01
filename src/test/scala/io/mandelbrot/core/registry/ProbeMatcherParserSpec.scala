package io.mandelbrot.core.registry

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.scalatest.Inside._

class ProbeMatcherParserSpec extends WordSpec with MustMatchers {

  "ProbeMatcherParser" must {

    "parse '*'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*")
      println(matcher)
      matcher.getClass must be === MatchesAll.getClass
    }

    "parse '*:*'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:*")
      println(matcher)
      inside(matcher) {
        case ProbeMatcher(scheme, location, path) =>
          scheme must be === Some(MatchAny)
          location must be === Some(MatchAny)
          path must be === None
      }
    }

    "parse '*:example.com'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("*:example.com")
      println(matcher)
      inside(matcher) {
        case ProbeMatcher(Some(MatchAny), Some(location: MatchExact), None) =>
          location.string must be === "example.com"
      }
    }

    "parse 'fqdn:*.com'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("fqdn:*.com")
      println(matcher)
      inside(matcher) {
        case ProbeMatcher(Some(scheme: MatchExact), Some(location: MatchGlob), None) =>
          scheme.string must be === "fqdn"
          location.regex.toString must be === """.*\Q.com\E"""
      }
    }

    "parse 'fqdn:example.com'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("fqdn:example.com")
      println(matcher)
      inside(matcher) {
        case ProbeMatcher(Some(scheme: MatchExact), Some(location: MatchExact), None) =>
          scheme.string must be === "fqdn"
          location.string must be === "example.com"
      }
    }

    "parse 'fqdn:example.com/foo'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("fqdn:example.com/foo")
      println(matcher)
      inside(matcher) {
        case ProbeMatcher(Some(scheme: MatchExact), Some(location: MatchExact), Some(path: PathMatcher)) =>
          scheme.string must be === "fqdn"
          location.string must be === "example.com"
          path.segments must be === Vector(MatchExact("foo"))
      }
    }

    "parse 'fqdn:example.com/foo/bar/baz'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("fqdn:example.com/foo/bar/baz")
      println(matcher)
      inside(matcher) {
        case ProbeMatcher(Some(scheme: MatchExact), Some(location: MatchExact), Some(path: PathMatcher)) =>
          scheme.string must be === "fqdn"
          location.string must be === "example.com"
          path.segments must be === Vector(MatchExact("foo"), MatchExact("bar"), MatchExact("baz"))
      }
    }

    "parse 'fqdn:example.com/foo/*'" in {
      val matcher = new ProbeMatcherParser().parseProbeMatcher("fqdn:example.com/foo/*")
      println(matcher)
      inside(matcher) {
        case ProbeMatcher(Some(scheme: MatchExact), Some(location: MatchExact), Some(path: PathMatcher)) =>
          scheme.string must be === "fqdn"
          location.string must be === "example.com"
          path.segments must be === Vector(MatchExact("foo"), MatchAny)
      }
    }

  }
}
