package io.mandelbrot.core.parser

import io.mandelbrot.core.model
import io.mandelbrot.core.model._
import org.scalatest.Inside._
import org.scalatest.{ShouldMatchers, WordSpec}

class CheckMatcherParserSpec extends WordSpec with ShouldMatchers {

  "CheckMatcherParser" should {

    "parse '*'" in {
      val matcher = CheckMatcherParser.parseCheckMatcher("*")
      println(matcher)
      inside(matcher) {
        case MatchesAll =>
      }
    }

    "parse 'foo'" in {
      val matcher = CheckMatcherParser.parseCheckMatcher("foo")
      println(matcher)
      inside(matcher) {
        case PathMatcher(segments) =>
          segments shouldEqual Vector(MatchExact("foo"))
      }
    }

    "parse 'foo.bar.baz'" in {
      val matcher = CheckMatcherParser.parseCheckMatcher("foo.bar.baz")
      println(matcher)
      inside(matcher) {
        case PathMatcher(segments) =>
          segments shouldEqual Vector(MatchExact("foo"), MatchExact("bar"), MatchExact("baz"))
      }
    }

    "parse 'foo.*'" in {
      val matcher = CheckMatcherParser.parseCheckMatcher("foo.*")
      println(matcher)
      inside(matcher) {
        case PathMatcher(segments) =>
          segments shouldEqual Vector(MatchExact("foo"), MatchAny)
      }
    }

  }
}
