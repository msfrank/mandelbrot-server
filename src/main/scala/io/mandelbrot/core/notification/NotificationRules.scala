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

import io.mandelbrot.core.registry.{ProbeRef, ProbeMatcher}

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
