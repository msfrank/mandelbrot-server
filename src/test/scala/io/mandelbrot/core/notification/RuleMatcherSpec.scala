package io.mandelbrot.core.notification

import io.mandelbrot.core.parser.{CheckMatcherParser, CheckMatcherParser$}
import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers
import org.joda.time.DateTime

import io.mandelbrot.core.check._
import io.mandelbrot.core.model._

class RuleMatcherSpec extends WordSpec with ShouldMatchers {

  "AnyMatcher" should {
    "match a notification" in {
      AnyMatcher.matches(NotifySquelched(CheckRef("foo.local:load"), DateTime.now())) shouldEqual true
    }
  }

  "CheckMatcher" should {
    "match when checkId matches" in {
      val matcher = CheckRuleMatcher(CheckMatcherParser.parseCheckMatcher("*"))
      matcher.matches(NotifyHealthExpires(CheckRef("foo.local:load"), DateTime.now(), None)) shouldEqual true
    }
    "not match when check ref doesn't match" in {
      val matcher = CheckRuleMatcher(CheckMatcherParser.parseCheckMatcher("load"))
      matcher.matches(NotifyHealthExpires(CheckRef("foo.local:cpu"), DateTime.now(), None)) shouldEqual false
    }
    "not match when notification is not a CheckNotification" in {

    }
  }

  "TypeMatcher" should {
    "match when kind matches" in {
      val matcher = TypeRuleMatcher("health-expires")
      matcher.matches(NotifyHealthExpires(CheckRef("foo.local:load"), DateTime.now(), None)) shouldEqual true
    }
    "not match when kind doesn't match" in {
      val matcher = TypeRuleMatcher("health-changes")
      matcher.matches(NotifyHealthExpires(CheckRef("foo.local:cpu"), DateTime.now(), None)) shouldEqual false
    }
  }

  "LifecycleMatcher" should {
    "match when lifecycle matches" in {
      val matcher = LifecycleRuleMatcher(CheckKnown)
      matcher.matches(NotifyLifecycleChanges(CheckRef("foo.local:load"), DateTime.now(), CheckJoining, CheckKnown)) shouldEqual true
    }
    "not match when lifecycle doesn't match" in {
      val matcher = LifecycleRuleMatcher(CheckKnown)
      matcher.matches(NotifyLifecycleChanges(CheckRef("foo.local:load"), DateTime.now(), CheckKnown, CheckJoining)) shouldEqual false
    }
    "not match when notification type is not NotifyLifecycleChanges" in {
      val matcher = LifecycleRuleMatcher(CheckKnown)
      matcher.matches(NotifyHealthExpires(CheckRef("foo.local:cpu"), DateTime.now(), None)) shouldEqual false
    }
  }

}
