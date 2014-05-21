package io.mandelbrot.core.notification

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import java.io.StringReader

import io.mandelbrot.core.registry.{MatchAny, MatchExact, PathMatcher, ProbeMatcher}

class NotificationRulesParserSpec extends WordSpec with MustMatchers {

  val user1 = Contact("user1", "User One", Map.empty)
  val user2 = Contact("user2", "User Two", Map.empty)
  val user3 = Contact("user3", "User Three", Map.empty)
  val user4 = Contact("user4", "User Four", Map.empty)
  val user5 = Contact("user5", "User Five", Map.empty)

  val contacts = Map("user1" -> user1, "user2" -> user2, "user3" -> user3, "user4" -> user4, "user5" -> user5)

  val groups = Map(
    "group1" -> Set(user4, user5)
  )

  "NotificationRulesParser" must {

    "parse a ruleset with multiple rules" in {
      val reader = new StringReader(
        """
          |when probe(*:*/load) : notify(@group1)
          |when probe(*:*/cpu)  : notify(user2, user3)
          |when probe(*:*/*)    : drop()
        """.stripMargin)
      val rules = NotificationRules.parse(reader, contacts, groups)
      rules.rules.length must be === 3
      rules.rules(0).matcher must be === ProbeRuleMatcher(ProbeMatcher(Some(MatchAny),Some(MatchAny),Some(PathMatcher(Vector(MatchExact("load"))))))
      rules.rules(0).action must be === NotifyContacts(Set(user4, user5))
      rules.rules(1).matcher must be === ProbeRuleMatcher(ProbeMatcher(Some(MatchAny),Some(MatchAny),Some(PathMatcher(Vector(MatchExact("cpu"))))))
      rules.rules(1).action must be === NotifyContacts(Set(user2, user3))
      rules.rules(2).matcher must be === ProbeRuleMatcher(ProbeMatcher(Some(MatchAny),Some(MatchAny),Some(PathMatcher(Vector(MatchAny)))))
      rules.rules(2).action must be === DropNotification
    }

  }

}
