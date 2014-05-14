package io.mandelbrot.core.registry

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.scalatest.Inside._

class ProbeMatcherParserSpec extends WordSpec with MustMatchers {

  "ProbeMatcherParser" must {

    "parse '*'" in {
      val matcher = ProbeMatcherParser("*")
      println(matcher)
      matcher.getClass must be === MatchesAll.getClass
    }

    "parse '*:*'" in {
      val matcher = ProbeMatcherParser("*:*")
      println(matcher)
      inside(matcher) {
        case ProbeMatcher(scheme, location, path) =>
          scheme must be === Some(MatchAny)
          location must be === Some(MatchAny)
          path must be === None
      }
    }

    "parse 'fqdn:*.com'" in {
      val matcher = ProbeMatcherParser("fqdn:*.com")
      println(matcher)
      inside(matcher) {
        case ProbeMatcher(Some(scheme: MatchExact), Some(location: MatchRegex), None) =>
          scheme.string must be === "fqdn"
          location.regex.toString must be === """.*\Q.com\E"""
      }
    }

    "parse 'fqdn:example.com'" in {
      val matcher = ProbeMatcherParser("fqdn:example.com")
      println(matcher)
      inside(matcher) {
        case ProbeMatcher(Some(scheme: MatchExact), Some(location: MatchExact), None) =>
          scheme.string must be === "fqdn"
          location.string must be === "example.com"
      }
    }

//    "parse 'fqdn:example.com/foo'" in {
//      val matcher = ProbeMatcherParser("fqdn:example.com/foo")
//      println(matcher)
//      inside(matcher) {
//        case ProbeMatcher(Some(scheme: MatchExact), Some(location: MatchExact), Some(path: PathMatcher)) =>
//          scheme.string must be === "fqdn"
//          location.string must be === "example.com"
//      }
//    }
  }
}
