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

import io.mandelbrot.core.registry.{ProbeLifecycle, ProbeHealth, ProbeRef}
import org.joda.time.DateTime
import java.util.UUID

/**
 * base trait for Notifications
 */
sealed trait Notification {
  val timestamp: DateTime
  val kind: String
  val description: String
  override def toString = kind
}

/**
 * A notification about a probe
 */
class ProbeNotification(val probeRef: ProbeRef, val timestamp: DateTime, val kind: String, val description: String, val correlation: Option[UUID]) extends Notification

object ProbeNotification {
  def apply(probeRef: ProbeRef, timestamp: DateTime, kind: String, description: String, correlation: Option[UUID]) = {
    new ProbeNotification(probeRef, timestamp, kind, description, correlation)
  }
  def unapply(notification: ProbeNotification): Option[(ProbeRef, DateTime, String, String, Option[UUID])] = {
    Some((notification.probeRef, notification.timestamp, notification.kind, notification.description, notification.correlation))
  }
}

/**
 *
 */
case class NotifyLifecycleChanges(override val probeRef: ProbeRef,
                                  override val timestamp: DateTime,
                                  oldLifecycle: ProbeLifecycle,
                                  newLifecycle: ProbeLifecycle)
extends ProbeNotification(probeRef, timestamp, "lifecycle-changes", "probe lifecycle transitions from %s to %s".format(oldLifecycle.toString, newLifecycle.toString), None)

/**
 *
 */
case class NotifyHealthChanges(override val probeRef: ProbeRef,
                               override val timestamp: DateTime,
                               override val correlation: Option[UUID],
                               oldHealth: ProbeHealth,
                               newHealth: ProbeHealth)
extends ProbeNotification(probeRef, timestamp, "health-changes", "probe transitions from %s to %s".format(oldHealth.toString, newHealth.toString), correlation)

/**
 *
 */
case class NotifyHealthUpdates(override val probeRef: ProbeRef,
                               override val timestamp: DateTime,
                               override val correlation: Option[UUID],
                               health: ProbeHealth)
extends ProbeNotification(probeRef, timestamp, "health-updates", "probe is " + health.toString, correlation)

/**
 *
 */
case class NotifyHealthExpires(override val probeRef: ProbeRef,
                               override val timestamp: DateTime,
                               override val correlation: Option[UUID])
extends ProbeNotification(probeRef, timestamp, "health-expires", "probe health expires", correlation)

/**
 *
 */
case class NotifyHealthAlerts(override val probeRef: ProbeRef,
                             override val timestamp: DateTime,
                             health: ProbeHealth,
                             correlationId: UUID,
                             acknowledgementId: Option[UUID])
extends ProbeNotification(probeRef, timestamp, "health-alerts", "probe is " + health.toString, Some(correlationId))

/**
 *
 */
case class NotifyHealthFlaps(override val probeRef: ProbeRef,
                             override val timestamp: DateTime,
                             override val correlation: Option[UUID],
                             flapStarts: DateTime)
extends ProbeNotification(probeRef, timestamp, "health-flaps", "probe health is flapping", correlation)

/**
 *
 */
case class NotifyAcknowledged(override val probeRef: ProbeRef,
                              override val timestamp: DateTime,
                              correlationId: UUID,
                              acknowledgementId: UUID)
extends ProbeNotification(probeRef, timestamp, "probe-acknowledged", "probe health is acknowledged", Some(correlationId))

/**
 *
 */
case class NotifySquelched(override val probeRef: ProbeRef,
                           override val timestamp: DateTime)
extends ProbeNotification(probeRef, timestamp, "probe-squelched", "probe notifications disabled", None)

/**
 *
 */
case class NotifyUnsquelched(override val probeRef: ProbeRef,
                             override val timestamp: DateTime)
extends ProbeNotification(probeRef, timestamp, "probe-unsquelched", "probe notifications enabled", None)
