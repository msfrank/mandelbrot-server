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
import java.io._

import io.mandelbrot.core.registry.{ProbeRef, ProbeMatcher}
import io.mandelbrot.core.registry.ProbeMatcher

/**
 * 
 */
sealed trait NotificationAction {
  import NotificationAction._
  def execute(notification: Notification, contacts: Set[Contact], notifiers: Map[String,ActorRef]): NotificationActionResult
}

object NotificationAction {
  sealed trait NotificationActionResult
  case object Continue extends NotificationActionResult
  case object Stop extends NotificationActionResult
}

/**
 *
 */
case object NotifyContacts extends NotificationAction {
  import NotificationAction.Continue
  def execute(notification: Notification, contacts: Set[Contact], notifiers: Map[String,ActorRef]) = {
    for (contact <- contacts; notifier <- notifiers.values) {
      notifier ! NotifyContact(contact, notification)
    }
    Continue
  }
}

/**
 *
 */
case object NotifyOnlyContacts extends NotificationAction {
  import NotificationAction.Stop
  def execute(notification: Notification, contacts: Set[Contact], notifiers: Map[String,ActorRef]) = {
    for (contact <- contacts; notifier <- notifiers.values) {
      notifier ! NotifyContact(contact, notification)
    }
    Stop
  }
}

/**
 *
 */
case object DropNotification extends NotificationAction {
  import NotificationAction.Stop
  def execute(notification: Notification, contacts: Set[Contact], notifiers: Map[String,ActorRef]) = Stop
}

/**
 * 
 */
case class NotificationRule(subject: Set[ProbeMatcher], action: NotificationAction, contacts: Set[Contact]) {
  def matches(probeRef: ProbeRef): Boolean = {
    subject.foreach { probeMatcher => if (probeMatcher.matches(probeRef)) return true }
    false
  }
  def execute(notification: Notification, notifiers: Map[String,ActorRef]) = action.execute(notification, contacts, notifiers)
}

/**
 * 
 */
class NotificationRules(rules: Vector[NotificationRule], notifiers: Map[String,ActorRef]) {
  import NotificationAction._
  def evaluate(notification: Notification): Unit = notification match {
    case n: ProbeNotification =>
      rules.foreach { case rule if rule.matches(n.probeRef) =>
        if (rule.execute(n, notifiers) == Stop)
          return
      }
    case unknown => // FIXME
  }
}

object NotificationRules {

  /**
   *
   */
  def parse(file: File): NotificationRules = parse(new FileReader(file))

  /**
   *
   */
  def parse(reader: Reader): NotificationRules = {
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
            rules = rules :+ parseRule(ruleString, lineNumber)
          rule = new StringBuilder()
          lineNumber = lines.getLineNumber
          rule.append(string.trim)
      }
      line = lines.readLine()
    }
    if (!rule.isEmpty)
      rules = rules :+ parseRule(rule.toString(), lineNumber)
    new NotificationRules(rules, Map.empty)
  }

  /**
   *
   */
  def parseRule(string: String, lineNumber: Int): NotificationRule = {
    println("rule @ line %d: %s".format(lineNumber, string))
    NotificationRule(Set.empty, DropNotification, Set.empty)
  }
}
