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

import akka.actor.{ActorRef, ActorContext}
import org.joda.time.DateTime
import scala.concurrent.duration.FiniteDuration

import io.mandelbrot.core.registry.{ProbeLifecycle, ProbeHealth, ProbeRef}

/**
 *
 */
trait NotificationPolicy {
  def notify(notification: Notification): Unit
}

/**
 *
 */
sealed trait Notification
case class NotifyHealthChanges(probeRef: ProbeRef, oldHealth: ProbeHealth, newHealth: ProbeHealth, timestamp: DateTime) extends Notification
case class NotifyHealthUpdates(probeRef: ProbeRef, health: ProbeHealth, timestamp: DateTime) extends Notification
case class NotifyHealthExpires(probeRef: ProbeRef, timestamp: DateTime) extends Notification
case class NotifyHealthFlaps(probeRef: ProbeRef, flapStarts: DateTime, timestamp: DateTime) extends Notification
case class NotifyLifecycleChanges(probeRef: ProbeRef, oldLifecycle: ProbeLifecycle, newLifecycle: ProbeLifecycle, timestamp: DateTime) extends Notification

class NotifyParentPolicy(implicit val parent: ActorRef) extends NotificationPolicy {
  def notify(notification: Notification): Unit = {
    parent ! notification
  }
}
