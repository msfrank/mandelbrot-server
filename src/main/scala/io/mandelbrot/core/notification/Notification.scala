package io.mandelbrot.core.notification

import io.mandelbrot.core.registry.{ProbeLifecycle, ProbeHealth, ProbeRef}
import org.joda.time.DateTime
import java.util.UUID

/**
 *
 */
sealed trait Notification

sealed trait ProbeNotification extends Notification { val probeRef: ProbeRef }
case class NotifyHealthChanges(probeRef: ProbeRef, oldHealth: ProbeHealth, newHealth: ProbeHealth, timestamp: DateTime) extends ProbeNotification
case class NotifyHealthUpdates(probeRef: ProbeRef, health: ProbeHealth, timestamp: DateTime) extends ProbeNotification
case class NotifyHealthExpires(probeRef: ProbeRef, timestamp: DateTime) extends ProbeNotification
case class NotifyHealthFlaps(probeRef: ProbeRef, flapStarts: DateTime, timestamp: DateTime) extends ProbeNotification
case class NotifyLifecycleChanges(probeRef: ProbeRef, oldLifecycle: ProbeLifecycle, newLifecycle: ProbeLifecycle, timestamp: DateTime) extends ProbeNotification

case class NotifyAcknowledged(probeRef: ProbeRef, correlationId: UUID, acknowledgementId: UUID, timestamp: DateTime) extends ProbeNotification
case class NotifySquelched(probeRef: ProbeRef, timestamp: DateTime) extends ProbeNotification
case class NotifyUnsquelched(probeRef: ProbeRef, timestamp: DateTime) extends ProbeNotification
