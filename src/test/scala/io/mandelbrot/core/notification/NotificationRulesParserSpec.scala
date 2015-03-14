package io.mandelbrot.core.notification

import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers
import java.io.StringReader

import io.mandelbrot.core.model._

class NotificationRulesParserSpec extends WordSpec with ShouldMatchers {

  val user1 = Contact("user1", "User One", Map.empty)
  val user2 = Contact("user2", "User Two", Map.empty)
  val user3 = Contact("user3", "User Three", Map.empty)
  val user4 = Contact("user4", "User Four", Map.empty)
  val user5 = Contact("user5", "User Five", Map.empty)

  val contacts = Map("user1" -> user1, "user2" -> user2, "user3" -> user3, "user4" -> user4, "user5" -> user5)

  val groups = Map(
    "group1" -> ContactGroup("group1", "Group One", Map.empty, Set(user4, user5))
  )

  "NotificationRulesParser" should {

    "parse a ruleset with multiple rules" in {
      val reader = new StringReader(
        """
          |when probe(*:*/load) : notify(@group1)
          |when probe(*:*/cpu)  : notify(user2, user3)
          |when probe(*:*/*)    : drop()
        """.stripMargin)
      val rules = NotificationRules.parse(reader, contacts, groups)
      rules.rules.length shouldEqual 3
      rules.rules(0).matcher shouldEqual ProbeRuleMatcher(ProbeMatcher(Some(MatchAny),Some(MatchAny),Some(PathMatcher(Vector(MatchExact("load"))))))
      rules.rules(0).action shouldEqual NotifyContacts(Set(user4, user5))
      rules.rules(1).matcher shouldEqual ProbeRuleMatcher(ProbeMatcher(Some(MatchAny),Some(MatchAny),Some(PathMatcher(Vector(MatchExact("cpu"))))))
      rules.rules(1).action shouldEqual NotifyContacts(Set(user2, user3))
      rules.rules(2).matcher shouldEqual ProbeRuleMatcher(ProbeMatcher(Some(MatchAny),Some(MatchAny),Some(PathMatcher(Vector(MatchAny)))))
      rules.rules(2).action shouldEqual DropNotification
    }

    "parse any matcher" in {
      val parser = new NotificationRuleParser(contacts, groups)
      val matcher = parser.parseAll(parser.anyMatcher, "any()")
      matcher.get should be(AnyMatcher)
    }

    "parse probe matcher" in {
      val parser = new NotificationRuleParser(contacts, groups)
      val matcher = parser.parseAll(parser.probeMatcher, "probe(*)")
      matcher.get shouldEqual ProbeRuleMatcher(ProbeMatcher(None, None, None))
    }

    "parse type matcher" in {
      val parser = new NotificationRuleParser(contacts, groups)
      val matcher = parser.parseAll(parser.typeMatcher, "type(probe-acknowledged)")
      matcher.get shouldEqual TypeRuleMatcher("probe-acknowledged")
    }

    "parse lifecycle matcher" in {
      val parser = new NotificationRuleParser(contacts, groups)
      val matcher = parser.parseAll(parser.lifecycleMatcher, "lifecycle(joining)")
      matcher.get shouldEqual LifecycleRuleMatcher(ProbeJoining)
    }

    "parse health matcher" in {
      val parser = new NotificationRuleParser(contacts, groups)
      val matcher = parser.parseAll(parser.healthMatcher, "health(healthy)")
      matcher.get shouldEqual HealthRuleMatcher(ProbeHealthy)
    }

    "parse alert matcher" in {
      val parser = new NotificationRuleParser(contacts, groups)
      val matcher = parser.parseAll(parser.alertMatcher, "alert(failed)")
      matcher.get shouldEqual AlertRuleMatcher(ProbeFailed)
    }

    "parse and operator expression" in {
      val parser = new NotificationRuleParser(contacts, groups)
      val matcher = parser.parseAll(parser.ruleExpression, "alert(failed) and lifecycle(known)")
      matcher.get shouldEqual AndOperator(Vector(AlertRuleMatcher(ProbeFailed), LifecycleRuleMatcher(ProbeKnown)))
    }

    "parse or operator expression" in {
      val parser = new NotificationRuleParser(contacts, groups)
      val matcher = parser.parseAll(parser.ruleExpression, "alert(failed) or lifecycle(known)")
      matcher.get shouldEqual OrOperator(Vector(AlertRuleMatcher(ProbeFailed), LifecycleRuleMatcher(ProbeKnown)))
    }

    "parse not operator expression" in {
      val parser = new NotificationRuleParser(contacts, groups)
      val matcher = parser.parseAll(parser.ruleExpression, "alert(failed) and not lifecycle(known)")
      matcher.get shouldEqual AndOperator(Vector(AlertRuleMatcher(ProbeFailed), NotOperator(LifecycleRuleMatcher(ProbeKnown))))
    }

    "parse group operator expression" in {
      val parser = new NotificationRuleParser(contacts, groups)
      val matcher = parser.parseAll(parser.ruleExpression, "(alert(failed) or alert(degraded)) and (lifecycle(known) or lifecycle(joining))")
      matcher.get shouldEqual AndOperator(Vector(
        OrOperator(Vector(AlertRuleMatcher(ProbeFailed), AlertRuleMatcher(ProbeDegraded))),
        OrOperator(Vector(LifecycleRuleMatcher(ProbeKnown), LifecycleRuleMatcher(ProbeJoining)))
      ))
    }
  }
}
