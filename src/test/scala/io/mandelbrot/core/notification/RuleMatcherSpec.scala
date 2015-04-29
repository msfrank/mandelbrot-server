package io.mandelbrot.core.notification

import io.mandelbrot.core.parser.{CheckMatcherParser, CheckMatcherParser$}
import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers
import org.joda.time.DateTime

import io.mandelbrot.core.system._
import io.mandelbrot.core.model._

class RuleMatcherSpec extends WordSpec with ShouldMatchers {

  "AnyMatcher" should {
    "match a notification" in {
      AnyMatcher.matches(NotifySquelched(ProbeRef("foo.local:load"), DateTime.now())) shouldEqual true
    }
  }

  "CheckMatcher" should {
    "match when checkId matches" in {
      val matcher = CheckRuleMatcher(CheckMatcherParser.parseCheckMatcher("*"))
      matcher.matches(NotifyHealthExpires(ProbeRef("foo.local:load"), DateTime.now(), None)) shouldEqual true
    }
    "not match when probe ref doesn't match" in {
      val matcher = CheckRuleMatcher(CheckMatcherParser.parseCheckMatcher("load"))
      matcher.matches(NotifyHealthExpires(ProbeRef("foo.local:cpu"), DateTime.now(), None)) shouldEqual false
    }
    "not match when notification is not a ProbeNotification" in {

    }
  }

  "TypeMatcher" should {
    "match when kind matches" in {
      val matcher = TypeRuleMatcher("health-expires")
      matcher.matches(NotifyHealthExpires(ProbeRef("foo.local:load"), DateTime.now(), None)) shouldEqual true
    }
    "not match when kind doesn't match" in {
      val matcher = TypeRuleMatcher("health-changes")
      matcher.matches(NotifyHealthExpires(ProbeRef("foo.local:cpu"), DateTime.now(), None)) shouldEqual false
    }
  }

  "LifecycleMatcher" should {
    "match when lifecycle matches" in {
      val matcher = LifecycleRuleMatcher(ProbeKnown)
      matcher.matches(NotifyLifecycleChanges(ProbeRef("foo.local:load"), DateTime.now(), ProbeJoining, ProbeKnown)) shouldEqual true
    }
    "not match when lifecycle doesn't match" in {
      val matcher = LifecycleRuleMatcher(ProbeKnown)
      matcher.matches(NotifyLifecycleChanges(ProbeRef("foo.local:load"), DateTime.now(), ProbeKnown, ProbeJoining)) shouldEqual false
    }
    "not match when notification type is not NotifyLifecycleChanges" in {
      val matcher = LifecycleRuleMatcher(ProbeKnown)
      matcher.matches(NotifyHealthExpires(ProbeRef("foo.local:cpu"), DateTime.now(), None)) shouldEqual false
    }
  }

}
