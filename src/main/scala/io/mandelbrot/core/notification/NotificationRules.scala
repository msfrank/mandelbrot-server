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
import java.io._

import io.mandelbrot.core.registry.ProbeMatcher
import org.slf4j.LoggerFactory

/**
 *
 */
sealed trait RuleMatcher {
  def matches(notification: Notification): Boolean
}

case object RuleMatchesAll extends RuleMatcher {
  def matches(notification: Notification): Boolean = true
}

/**
 *
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
 *
 */
case class TypeRuleMatcher(notificationType: Class[_]) extends RuleMatcher {
  def matches(notification: Notification): Boolean = false
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
  def parse(file: File, contacts: Map[String,Contact], groups: Map[String,Set[Contact]]): NotificationRules = {
    parse(new FileReader(file), contacts, groups)
  }

  /**
   *
   */
  def parse(reader: Reader, contacts: Map[String,Contact], groups: Map[String,Set[Contact]]): NotificationRules = {
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
class NotificationRuleParser(contacts: Map[String,Contact], groups: Map[String,Set[Contact]]) extends JavaTokenParsers {
  import io.mandelbrot.core.registry.ProbeMatcherParser

  val logger = LoggerFactory.getLogger(classOf[NotificationRuleParser])

  /* shamelessly copied from Parsers.scala */
  def _log[T](p: => Parser[T])(name: String): Parser[T] = Parser { in =>
    logger.debug("trying " + name + " at "+ in)
    val r = p(in)
    logger.debug(name + " --> " + r)
    r
  }

  // matches a double-quoted string, removes the quotes
  def unwrappedStringLiteral: Parser[String] = stringLiteral ^^ (_.tail.init)

  /*
   *
   */
  val probeMatcherParser = new ProbeMatcherParser()
  def probeMatcher: Parser[RuleMatcher] = _log(ident ~ literal("(") ~ regex("""[^)]*""".r) ~ literal(")"))("probeMatcher") ^^ {
    case "probe" ~ "(" ~ matchString ~ ")" =>
      ProbeRuleMatcher(probeMatcherParser.parseProbeMatcher(matchString.trim))
  }

  def ruleMatcher: Parser[RuleMatcher] = _log(probeMatcher)("ruleMatcher")

  /*
   * parse a mixed parameter list of contacts and groups
   */
  def group: Parser[Set[Contact]] = _log(literal("@") ~ (ident | unwrappedStringLiteral))("group") ^^ {
    case "@" ~ name =>
      groups.get(name) match {
        case Some(_group) => _group
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
      NotifyContacts(targets)
  }
  def dropAction: Parser[RuleAction] = _log(literal("drop") ~ literal("(") ~ literal(")"))("dropAction") ^^ {
    case "drop" ~ "(" ~ ")" =>
      DropNotification
  }

  def ruleAction: Parser[RuleAction] = _log(notifyAction | notifyOnlyAction | dropAction)("ruleAction")

  /* */
  def notificationRule: Parser[NotificationRule] = _log(literal("when") ~ ruleMatcher ~ ":" ~ ruleAction)("notificationRule") ^^ {
    case "when" ~ matcher ~ ":" ~ action =>
      NotificationRule(matcher, action)
  }

  def parseRule(input: String): NotificationRule = parseAll(notificationRule, input) match {
    case Success(rule: NotificationRule, _) => rule
    case Success(other, _) => throw new Exception("unexpected parse result")
    case failure : NoSuccess => throw new Exception(failure.msg)
  }
}