/**
 * Copyright 2014 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Mandelbrot.
 *
 * Mandelbrot is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Mandelbrot is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Mandelbrot.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mandelbrot.core.notification

import akka.actor.ActorRef
import scala.util.parsing.combinator.JavaTokenParsers
import org.slf4j.LoggerFactory
import java.io._

import io.mandelbrot.core.registry._

/**
 * interface for implementing a rule matcher.
 */
sealed trait RuleMatcher {
  def matches(notification: Notification): Boolean
}

/**
 * 'any' matches all notifications.
 */
case object AnyMatcher extends RuleMatcher {
  def matches(notification: Notification): Boolean = true
}

/**
 * 'probe' matches if notification is a ProbeNotification and probeRef matches.
 */
case class ProbeRuleMatcher(matcher: ProbeMatcher) extends RuleMatcher {
  def matches(notification: Notification): Boolean = notification match {
    case n: ProbeNotification if matcher.matches(n.probeRef) =>
      true
    case otherwise =>
      false
  }
}

/**
 * 'type' matches if notification kind matches.
 */
case class TypeRuleMatcher(kind: String) extends RuleMatcher {
  def matches(notification: Notification): Boolean = notification.kind == kind
}

/**
 * 'lifecycle' matches if notification is of type NotifyLifecycleChanges and newLifecycle matches.
 */
case class LifecycleRuleMatcher(lifecycle: ProbeLifecycle) extends RuleMatcher {
  def matches(notification: Notification): Boolean = notification match {
    case n: NotifyLifecycleChanges if n.newLifecycle == lifecycle =>
      true
    case otherwise =>
      false
  }
}

/**
 * 'health' matches if notification is of type NotifyHealthChanges and newHealth matches.
 */
case class HealthRuleMatcher(health: ProbeHealth) extends RuleMatcher {
  def matches(notification: Notification): Boolean = notification match {
    case n: NotifyHealthChanges if n.newHealth == health =>
      true
    case otherwise =>
      false
  }
}

/**
 * 'alert' matches if notification is of type NotifyHealthAlerts and health matches.
 */
case class AlertRuleMatcher(health: ProbeHealth) extends RuleMatcher {
  def matches(notification: Notification): Boolean = notification match {
    case n: NotifyHealthAlerts if n.health == health =>
      true
    case otherwise =>
      false
  }
}

/**
 * 'and' matches if and only if all of its children match.
 */
case class AndOperator(children: Vector[RuleMatcher]) extends RuleMatcher {
  def matches(notification: Notification): Boolean = {
    children.foreach(child => if (!child.matches(notification)) return false)
    true
  }
}

/**
 * 'or' matches if any of its children match.
 */
case class OrOperator(children: Vector[RuleMatcher]) extends RuleMatcher {
  def matches(notification: Notification): Boolean = {
    children.foreach(child => if (child.matches(notification)) return true)
    false
  }
}

/**
 * 'not' matches if its child does _not_ match.
 */
case class NotOperator(child: RuleMatcher) extends RuleMatcher {
  def matches(notification: Notification): Boolean = !child.matches(notification)
}


/**
 * 
 */
sealed trait RuleAction {
  def execute(notification: Notification, notifiers: Map[String,ActorRef]): Option[Notification]
}

/**
 *
 */
case class NotifyContacts(contacts: Set[Contact]) extends RuleAction {
  def execute(notification: Notification, notifiers: Map[String,ActorRef]) = {
    for (contact <- contacts; notifier <- notifiers.values) {
      notifier ! NotifyContact(contact, notification)
    }
    Some(notification)
  }
}

/**
 *
 */
case class NotifyOnlyContacts(contacts: Set[Contact]) extends RuleAction {
  def execute(notification: Notification, notifiers: Map[String,ActorRef]) = {
    for (contact <- contacts; notifier <- notifiers.values) {
      notifier ! NotifyContact(contact, notification)
    }
    None
  }
}

/**
 *
 */
case object DropNotification extends RuleAction {
  def execute(notification: Notification, notifiers: Map[String,ActorRef]) = None
}

/**
 * 
 */
case class NotificationRule(matcher: RuleMatcher, action: RuleAction) {
  def matches(notification: Notification): Boolean = matcher.matches(notification)
  def execute(notification: Notification, notifiers: Map[String,ActorRef]) = {
    action.execute(notification, notifiers)
  }
}

/**
 * 
 */
class NotificationRules(val rules: Vector[NotificationRule]) {
  def evaluate(notification: Notification, notifiers: Map[String,ActorRef]): Unit = {
    var current: Notification = notification
    for (rule <- rules if rule.matches(current)) {
      rule.execute(current, notifiers) match {
        case Some(modified) =>
          current = modified
        case None =>
          return
      }
    }
  }
}

object NotificationRules {

  /**
   *
   */
  def parse(file: File, contacts: Map[String,Contact], groups: Map[String,ContactGroup]): NotificationRules = {
    parse(new FileReader(file), contacts, groups)
  }

  /**
   *
   */
  def parse(reader: Reader, contacts: Map[String,Contact], groups: Map[String,ContactGroup]): NotificationRules = {
    val parser = new NotificationRuleParser(contacts, groups)
    var rules = Vector.empty[NotificationRule]
    val lines = new LineNumberReader(reader)
    var rule = new StringBuilder()
    var line = lines.readLine()
    var lineNumber = lines.getLineNumber
    while (line != null) {
      line match {
        case empty if line.trim.isEmpty =>            // ignore blank lines
        case comment if line.trim.startsWith("#") =>  // ignore lines starting with comment char '#'
        case continuation if line(0).isSpaceChar =>   // process continuation
          if (rule.isEmpty) {
            lineNumber = lines.getLineNumber
            rule.append(line.trim)
          } else rule.append(" " + line.trim)
        case string =>                                // process new rule
          val ruleString = rule.toString()
          if (!rule.isEmpty)
            rules = rules :+ parser.parseRule(ruleString)
          rule = new StringBuilder()
          lineNumber = lines.getLineNumber
          rule.append(string.trim)
      }
      line = lines.readLine()
    }
    if (!rule.isEmpty)
      rules = rules :+ parser.parseRule(rule.toString())
    new NotificationRules(rules)
  }
}

/**
 *
 */
class NotificationRuleParser(contacts: Map[String,Contact], groups: Map[String,ContactGroup]) extends JavaTokenParsers {
  import io.mandelbrot.core.registry.ProbeMatcherParser

  val logger = LoggerFactory.getLogger(classOf[NotificationRuleParser])

  /* shamelessly copied from Parsers.scala */
  def _log[T](p: => Parser[T])(name: String): Parser[T] = Parser { in =>
    //logger.debug("trying " + name + " at "+ in)
    val r = p(in)
    //logger.debug(name + " --> " + r)
    r
  }

  // matches a double-quoted string, removes the quotes
  def unwrappedStringLiteral: Parser[String] = stringLiteral ^^ (_.tail.init)

  /*
   *
   */
  def anyMatcher: Parser[RuleMatcher] = _log(literal("any") ~ literal("(") ~ literal(")"))("anyMatcher") ^^ {
    case "any" ~ "(" ~ ")" => AnyMatcher
  }

  val probeMatcherParser = new ProbeMatcherParser()
  def probeMatcher: Parser[RuleMatcher] = _log(literal("probe") ~ literal("(") ~ regex("""[^)]*""".r) ~ literal(")"))("probeMatcher") ^^ {
    case "probe" ~ "(" ~ matchString ~ ")" =>
      ProbeRuleMatcher(probeMatcherParser.parseProbeMatcher(matchString.trim))
  }

  def typeMatcher: Parser[RuleMatcher] = _log(literal("type") ~ literal("(") ~ regex("""[^)]*""".r) ~ literal(")"))("typeMatcher") ^^ {
    case "type" ~ "(" ~ matchString ~ ")" =>
      TypeRuleMatcher(matchString.trim)
  }

  def lifecycleMatcher: Parser[RuleMatcher] = _log(literal("lifecycle") ~ literal("(") ~ regex("""[^)]*""".r) ~ literal(")"))("lifecycleMatcher") ^^ {
    case "lifecycle" ~ "(" ~ matchString ~ ")" =>
      matchString.trim match {
        case "joining" =>
          LifecycleRuleMatcher(ProbeJoining)
        case "known" =>
          LifecycleRuleMatcher(ProbeKnown)
        case "retired" =>
          LifecycleRuleMatcher(ProbeRetired)
        case unknown =>
          throw new Exception("unknown lifecycle '%s'".format(unknown))
      }
  }

  def healthMatcher: Parser[RuleMatcher] = _log(literal("health") ~ literal("(") ~ regex("""[^)]*""".r) ~ literal(")"))("healthMatcher") ^^ {
    case "health" ~ "(" ~ matchString ~ ")" =>
      matchString.trim match {
        case "healthy" =>
          HealthRuleMatcher(ProbeHealthy)
        case "degraded" =>
          HealthRuleMatcher(ProbeDegraded)
        case "failed" =>
          HealthRuleMatcher(ProbeFailed)
        case "unknown" =>
          HealthRuleMatcher(ProbeUnknown)
        case unknown =>
          throw new Exception("unknown health '%s'".format(unknown))
      }
  }

  def alertMatcher: Parser[RuleMatcher] = _log(literal("alert") ~ literal("(") ~ regex("""[^)]*""".r) ~ literal(")"))("healthMatcher") ^^ {
    case "alert" ~ "(" ~ matchString ~ ")" =>
      matchString.trim match {
        case "healthy" =>
          AlertRuleMatcher(ProbeHealthy)
        case "degraded" =>
          AlertRuleMatcher(ProbeDegraded)
        case "failed" =>
          AlertRuleMatcher(ProbeFailed)
        case "unknown" =>
          AlertRuleMatcher(ProbeUnknown)
        case unknown =>
          throw new Exception("unknown health '%s'".format(unknown))
      }
  }

  def ruleMatcher: Parser[RuleMatcher] = _log(anyMatcher | probeMatcher | typeMatcher | lifecycleMatcher | healthMatcher | alertMatcher)("ruleMatcher")


  /*
   * <Query>        ::= <OrOperator>
   * <OrOperator>   ::= <AndOperator> ('OR' <AndOperator>)*
   * <AndOperator>  ::= <NotOperator> ('AND' <NotOperator>)*
   * <NotOperator>  ::= ['NOT'] <NotOperator> | <Group>
   * <Group>        ::= '(' <OrOperator> ')' | <Expression>
   */

  def groupOperator: Parser[RuleMatcher] = _log((literal("(") ~> orOperator <~ literal(")")) | ruleMatcher)("groupOperator") ^^ {
    case group: RuleMatcher => group
  }

  def notOperator: Parser[RuleMatcher] = _log(("not" ~ notOperator) | groupOperator)("notOperator") ^^ {
    case "not" ~ (not: RuleMatcher) => NotOperator(not)
    case group: RuleMatcher => group
  }

  def andOperator: Parser[RuleMatcher] = _log(notOperator ~ rep("and" ~ notOperator))("andOperator") ^^ {
    case not1 ~ nots if nots.isEmpty =>
      not1
    case not1 ~ nots =>
      val children: Vector[RuleMatcher] = nots.map { case "and" ~ group => group }.toVector
      AndOperator(not1 +: children)
  }

  def orOperator: Parser[RuleMatcher] = _log(andOperator ~ rep("or" ~ andOperator))("orOperator") ^^ {
    case and1 ~ ands if ands.isEmpty =>
      and1
    case and1 ~ ands =>
      val children: Vector[RuleMatcher] = ands.map { case "or" ~ group => group }.toVector
      OrOperator(and1 +: children)
  }

  /* the entry point */
  val ruleExpression: Parser[RuleMatcher] = _log(orOperator)("ruleExpression")


  /*
   * parse a mixed parameter list of contacts and groups
   */
  def group: Parser[Set[Contact]] = _log(literal("@") ~ (ident | unwrappedStringLiteral))("group") ^^ {
    case "@" ~ name =>
      groups.get(name) match {
        case Some(_group) => _group.contacts
        case None => Set.empty
      }
  }
  def contact: Parser[Set[Contact]] = _log(ident | unwrappedStringLiteral)("contact") ^^ {
    case name =>
      contacts.get(name) match {
        case Some(_contact) => Set(_contact)
        case None => Set.empty
      }
  }
  def contactOrGroup: Parser[Set[Contact]] = _log(group | contact)("contactOrGroup")
  def contactParams: Parser[Set[Contact]] = _log(repsep(contactOrGroup, literal(",")))("contactParams") ^^ {
    case list => list.flatten.toSet
  }


  /*
   * match actions
   */
  def notifyAction: Parser[RuleAction] = _log(literal("notify") ~ literal("(") ~ contactParams ~ literal(")"))("notifyAction") ^^ {
    case "notify" ~ "(" ~ targets ~ ")" =>
      NotifyContacts(targets)
  }
  def notifyOnlyAction: Parser[RuleAction] = _log(literal("notify-only") ~ literal("(") ~ contactParams ~ literal(")"))("notifyOnlyAction") ^^ {
    case "notify-only" ~ "(" ~ targets ~ ")" =>
      NotifyOnlyContacts(targets)
  }
  def dropAction: Parser[RuleAction] = _log(literal("drop") ~ literal("(") ~ literal(")"))("dropAction") ^^ {
    case "drop" ~ "(" ~ ")" =>
      DropNotification
  }

  def ruleAction: Parser[RuleAction] = _log(notifyAction | notifyOnlyAction | dropAction)("ruleAction")

  /* */
  def whenClause: Parser[NotificationRule] = _log(literal("when") ~ ruleExpression ~ ":" ~ ruleAction)("whenClause") ^^ {
    case "when" ~ matcher ~ ":" ~ action =>
      NotificationRule(matcher, action)
  }

  /* */
  def unlessClause: Parser[NotificationRule] = _log(literal("unless") ~ ruleExpression ~ ":" ~ ruleAction)("unlessClause") ^^ {
    case "unless" ~ matcher ~ ":" ~ action =>
      NotificationRule(NotOperator(matcher), action)
  }

  /* */
  def notificationRule: Parser[NotificationRule] = whenClause | unlessClause

  def parseRule(input: String): NotificationRule = parseAll(notificationRule, input) match {
    case Success(rule: NotificationRule, _) => rule
    case Success(other, _) => throw new Exception("unexpected parse result")
    case failure : NoSuccess => throw new Exception(failure.msg)
  }
}