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

import io.mandelbrot.core.registry.{ProbeState, ProbeRef}

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
case class NotifyStateChanges(probeRef: ProbeRef, oldState: ProbeState, newState: ProbeState) extends Notification
case class NotifyStateUpdates(probeRef: ProbeRef, state: ProbeState, updated: DateTime) extends Notification
case class NotifyStateTimeout(probeRef: ProbeRef, state: ProbeState, duration: FiniteDuration) extends Notification
case class NotifyStateFlaps(probeRef: ProbeRef, flapStarts: DateTime) extends Notification

class NotifyParentPolicy(implicit val parent: ActorRef) extends NotificationPolicy {
  def notify(notification: Notification): Unit = {
    parent ! notification
  }
}
