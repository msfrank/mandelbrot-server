package io.mandelbrot.core.notification

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers
import org.joda.time.DateTime

import io.mandelbrot.core.system._

class RuleMatcherSpec extends WordSpec with ShouldMatchers {

  "AnyMatcher" should {
    "match a notification" in {
      AnyMatcher.matches(NotifySquelched(ProbeRef("fqdn:localhost/load"), DateTime.now())) should be(true)
    }
  }

  "ProbeMatcher" should {
    "match when probe ref matches" in {
      val parser = new ProbeMatcherParser()
      val matcher = ProbeRuleMatcher(parser.parseProbeMatcher("*"))
      matcher.matches(NotifyHealthExpires(ProbeRef("fqdn:localhost/load"), DateTime.now(), None)) should be(true)
    }
    "not match when probe ref doesn't match" in {
      val parser = new ProbeMatcherParser()
      val matcher = ProbeRuleMatcher(parser.parseProbeMatcher("*:*/load"))
      matcher.matches(NotifyHealthExpires(ProbeRef("fqdn:localhost/cpu"), DateTime.now(), None)) should be(false)
    }
    "not match when notification is not a ProbeNotification" in {

    }
  }

  "TypeMatcher" should {
    "match when kind matches" in {
      val matcher = TypeRuleMatcher("health-expires")
      matcher.matches(NotifyHealthExpires(ProbeRef("fqdn:localhost/load"), DateTime.now(), None)) should be(true)
    }
    "not match when kind doesn't match" in {
      val matcher = TypeRuleMatcher("health-changes")
      matcher.matches(NotifyHealthExpires(ProbeRef("fqdn:localhost/cpu"), DateTime.now(), None)) should be(false)
    }
  }

  "LifecycleMatcher" should {
    "match when lifecycle matches" in {
      val matcher = LifecycleRuleMatcher(ProbeKnown)
      matcher.matches(NotifyLifecycleChanges(ProbeRef("fqdn:localhost/load"), DateTime.now(), ProbeJoining, ProbeKnown)) should be(true)
    }
    "not match when lifecycle doesn't match" in {
      val matcher = LifecycleRuleMatcher(ProbeKnown)
      matcher.matches(NotifyLifecycleChanges(ProbeRef("fqdn:localhost/load"), DateTime.now(), ProbeKnown, ProbeJoining)) should be(false)
    }
    "not match when notification type is not NotifyLifecycleChanges" in {
      val matcher = LifecycleRuleMatcher(ProbeKnown)
      matcher.matches(NotifyHealthExpires(ProbeRef("fqdn:localhost/cpu"), DateTime.now(), None)) should be(false)
    }
  }

}
