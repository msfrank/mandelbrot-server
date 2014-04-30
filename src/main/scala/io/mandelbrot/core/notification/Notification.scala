package io.mandelbrot.core.notification

import io.mandelbrot.core.registry.{ProbeLifecycle, ProbeHealth, ProbeRef}
import org.joda.time.DateTime
import java.util.UUID

/**
 *
 */
sealed trait Notification

class ProbeNotification(val probeRef: ProbeRef, val timestamp: DateTime, val description: String, val correlation: Option[UUID]) extends Notification

object ProbeNotification {
  def apply(probeRef: ProbeRef, timestamp: DateTime, description: String, correlation: Option[UUID]) = {
    new ProbeNotification(probeRef, timestamp, description, correlation)
  }
  def unapply(notification: ProbeNotification): Option[(ProbeRef, DateTime, String, Option[UUID])] = {
    Some((notification.probeRef, notification.timestamp, notification.description, notification.correlation))
  }
}

/**
 *
 */
case class NotifyLifecycleChanges(override val probeRef: ProbeRef,
                                  override val timestamp: DateTime,
                                  oldLifecycle: ProbeLifecycle,
                                  newLifecycle: ProbeLifecycle)
extends ProbeNotification(probeRef, timestamp, "probe lifecycle transitions from %s to %s".format(oldLifecycle.toString, newLifecycle.toString), None)

/**
 *
 */
case class NotifyHealthChanges(override val probeRef: ProbeRef,
                               override val timestamp: DateTime,
                               override val correlation: Option[UUID],
                               oldHealth: ProbeHealth,
                               newHealth: ProbeHealth)
extends ProbeNotification(probeRef, timestamp, "probe transitions from %s to %s".format(oldHealth.toString, newHealth.toString), correlation)

/**
 *
 */
case class NotifyHealthUpdates(override val probeRef: ProbeRef,
                               override val timestamp: DateTime,
                               override val correlation: Option[UUID],
                               health: ProbeHealth)
extends ProbeNotification(probeRef, timestamp, "probe is " + health.toString, correlation)

/**
 *
 */
case class NotifyHealthExpires(override val probeRef: ProbeRef,
                               override val timestamp: DateTime,
                               override val correlation: Option[UUID])
extends ProbeNotification(probeRef, timestamp, "probe health expires", correlation)

/**
 *
 */
case class NotifyHealthFlaps(override val probeRef: ProbeRef,
                             override val timestamp: DateTime,
                             override val correlation: Option[UUID],
                             flapStarts: DateTime)
extends ProbeNotification(probeRef, timestamp, "probe health is flapping", correlation)

/**
 *
 */
case class NotifyAcknowledged(override val probeRef: ProbeRef,
                              override val timestamp: DateTime,
                              correlationId: UUID,
                              acknowledgementId: UUID)
extends ProbeNotification(probeRef, timestamp, "probe health is acknowledged", Some(correlationId))

/**
 *
 */
case class NotifySquelched(override val probeRef: ProbeRef,
                           override val timestamp: DateTime)
extends ProbeNotification(probeRef, timestamp, "probe notifications disabled", None)

/**
 *
 */
case class NotifyUnsquelched(override val probeRef: ProbeRef,
                             override val timestamp: DateTime)
extends ProbeNotification(probeRef, timestamp, "probe notifications enabled", None)
