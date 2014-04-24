package io.mandelbrot.core.notification

import io.mandelbrot.core.registry.{ProbeLifecycle, ProbeHealth, ProbeRef}
import org.joda.time.DateTime
import java.util.UUID

/**
 *
 */
sealed trait Notification

sealed trait ProbeNotification extends Notification {
  val probeRef: ProbeRef
  val timestamp: DateTime
  val correlationId: Option[UUID]
  def description: String
}

case class NotifyLifecycleChanges(probeRef: ProbeRef, oldLifecycle: ProbeLifecycle, newLifecycle: ProbeLifecycle, timestamp: DateTime) extends ProbeNotification {
  val correlationId = None
  def description = "probe lifecycle transitions from %s to %s".format(oldLifecycle.value, newLifecycle.value)
}

case class NotifyHealthChanges(probeRef: ProbeRef, oldHealth: ProbeHealth, newHealth: ProbeHealth, correlationId: Option[UUID], timestamp: DateTime) extends ProbeNotification {
  def description = "probe transitions from %s to %s".format(oldHealth.value, newHealth.value)
}

case class NotifyHealthUpdates(probeRef: ProbeRef, health: ProbeHealth, correlationId: Option[UUID], timestamp: DateTime) extends ProbeNotification {
  def description = "probe is " + health.value
}

case class NotifyHealthExpires(probeRef: ProbeRef, correlationId: Option[UUID], timestamp: DateTime) extends ProbeNotification {
  def description = "probe health expires"
}

case class NotifyHealthFlaps(probeRef: ProbeRef, flapStarts: DateTime, correlationId: Option[UUID], timestamp: DateTime) extends ProbeNotification {
  def description = "probe health is flapping"
}

case class NotifyAcknowledged(probeRef: ProbeRef, correlation: UUID, acknowledgement: UUID, timestamp: DateTime) extends ProbeNotification {
  val correlationId = Some(correlation)
  def description = "probe health is acknowledged"
}

case class NotifySquelched(probeRef: ProbeRef, timestamp: DateTime) extends ProbeNotification {
  val correlationId = None
  def description = "probe notifications disabled"
}

case class NotifyUnsquelched(probeRef: ProbeRef, timestamp: DateTime) extends ProbeNotification {
  val correlationId = None
  def description = "probe notifications enabled"
}
