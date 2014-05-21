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

import akka.actor.{ActorRef, Props, ActorLogging, Actor}

import io.mandelbrot.core.{ServiceExtension, ServerConfig}
import io.mandelbrot.core.history.HistoryService
import io.mandelbrot.core.registry.ProbeMatcherParser

class NotificationManager extends Actor with ActorLogging {

  // config
  val settings = ServerConfig(context.system).settings.notification
  val notifiers: Map[String,ActorRef] = settings.notifiers.map { case (name, notifierSettings) =>
    val props = ServiceExtension.makePluginProps(notifierSettings.plugin, notifierSettings.settings)
    log.info("loading notifier plugin {}", name)
    name -> context.actorOf(props, name)
  }
  val rules = new NotificationRules(Vector(
    NotificationRule(RuleMatchesAll, NotifyContacts(settings.contacts.values.toSet))
  ))

  val historyService = HistoryService(context.system)

  def receive = {

    case notification: Notification =>
      rules.evaluate(notification, notifiers)
      historyService ! notification
  }

}

object NotificationManager {
  def props() = Props(classOf[NotificationManager])
}

/* marker trait for Notifier implementations */
trait Notifier