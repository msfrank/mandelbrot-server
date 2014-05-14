package io.mandelbrot.core.registry

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.scalatest.Inside._

class ProbeMatcherParserSpec extends WordSpec with MustMatchers {

  "ProbeMatcherParser" must {

    "parse '*'" in {
      val matcher = ProbeMatcherParser("*")
      matcher.getClass must be === MatchesAll.getClass
    }

    "parse '*:*'" in {
      val matcher = ProbeMatcherParser("*:*")
      inside(matcher) {
        case ProbeMatcher(scheme, location, path) =>
          scheme must be === Some(MatchAny)
          location must be === Some(MatchAny)
          path must be === None
      }
    }

//    "parse 'fqdn:*.com'" in {
//      val matcher = ProbeMatcherParser("fqdn:*.com")
//      inside(matcher) {
//        case ProbeMatcher(scheme, location, path) =>
//          scheme must be === Some(MatchExact("fqdn"))
//          location must be === Some(MatchRegex(".*\.com"))
//          path must be === None
//      }
//    }

  }
}
