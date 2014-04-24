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
  val correlation: Option[UUID]
  def description: String
}

case class NotifyLifecycleChanges(probeRef: ProbeRef, oldLifecycle: ProbeLifecycle, newLifecycle: ProbeLifecycle, timestamp: DateTime) extends ProbeNotification {
  val correlation = None
  def description = "probe lifecycle transitions from %s to %s".format(oldLifecycle.value, newLifecycle.value)
}

case class NotifyHealthChanges(probeRef: ProbeRef, oldHealth: ProbeHealth, newHealth: ProbeHealth, correlation: Option[UUID], timestamp: DateTime) extends ProbeNotification {
  def description = "probe transitions from %s to %s".format(oldHealth.value, newHealth.value)
}

case class NotifyHealthUpdates(probeRef: ProbeRef, health: ProbeHealth, correlation: Option[UUID], timestamp: DateTime) extends ProbeNotification {
  def description = "probe is " + health.value
}

case class NotifyHealthExpires(probeRef: ProbeRef, correlation: Option[UUID], timestamp: DateTime) extends ProbeNotification {
  def description = "probe health expires"
}

case class NotifyHealthFlaps(probeRef: ProbeRef, flapStarts: DateTime, correlation: Option[UUID], timestamp: DateTime) extends ProbeNotification {
  def description = "probe health is flapping"
}

case class NotifyAcknowledged(probeRef: ProbeRef, correlationId: UUID, acknowledgementId: UUID, timestamp: DateTime) extends ProbeNotification {
  val correlation = Some(correlationId)
  def description = "probe health is acknowledged"
}

case class NotifySquelched(probeRef: ProbeRef, timestamp: DateTime) extends ProbeNotification {
  val correlation = None
  def description = "probe notifications disabled"
}

case class NotifyUnsquelched(probeRef: ProbeRef, timestamp: DateTime) extends ProbeNotification {
  val correlation = None
  def description = "probe notifications enabled"
}
