package io.mandelbrot.core.model

import org.joda.time.DateTime
import java.util.UUID

import io.mandelbrot.core.ServiceEvent

sealed trait NotificationModel

/**
 * base trait for Notifications
 */
sealed trait NotificationEvent extends ServiceEvent with NotificationModel {
  val timestamp: DateTime
  val kind: String
  val description: String
  override def toString = kind
}

/**
 * A notification about a check
 */
class CheckNotification(val checkRef: CheckRef, val timestamp: DateTime, val kind: String, val description: String, val correlation: Option[UUID]) extends NotificationEvent {
  override def equals(other: Any): Boolean = other match {
    case notification: CheckNotification =>
      if (!checkRef.equals(notification.checkRef)) return false
      if (!timestamp.equals(notification.timestamp)) return false
      if (!kind.equals(notification.kind)) return false
      if (!description.equals(notification.description)) return false
      if (!correlation.equals(notification.correlation)) return false
      true
    case _ => false
  }
  override def toString = "%s: %s %s".format(kind, checkRef, description)
}

object CheckNotification {
  def apply(checkRef: CheckRef, timestamp: DateTime, kind: String, description: String, correlation: Option[UUID]) = {
    new CheckNotification(checkRef, timestamp, kind, description, correlation)
  }
  def unapply(notification: CheckNotification): Option[(CheckRef, DateTime, String, String, Option[UUID])] = {
    Some((notification.checkRef, notification.timestamp, notification.kind, notification.description, notification.correlation))
  }
}

/**
 * An alert about a check
 */
sealed trait Alert extends NotificationEvent {
  val checkRef: CheckRef
  val correlationId: UUID
}

/**
 *
 */
case class NotifyHealthAlerts(override val checkRef: CheckRef,
                             override val timestamp: DateTime,
                             health: CheckHealth,
                             correlationId: UUID,
                             acknowledgementId: Option[UUID])
extends CheckNotification(checkRef, timestamp, "health-alerts", "check is " + health.toString, Some(correlationId)) with Alert

/**
 *
 */
case class NotifyAcknowledged(override val checkRef: CheckRef,
                              override val timestamp: DateTime,
                              correlationId: UUID,
                              acknowledgementId: UUID)
extends CheckNotification(checkRef, timestamp, "check-acknowledged", "check health is acknowledged", Some(correlationId)) with Alert

/**
 *
 */
case class NotifyUnacknowledged(override val checkRef: CheckRef,
                                override val timestamp: DateTime,
                                correlationId: UUID,
                                acknowledgementId: UUID)
extends CheckNotification(checkRef, timestamp, "check-unacknowledged", "check health is unacknowledged", Some(correlationId)) with Alert

/**
 *
 */
case class NotifyRecovers(override val checkRef: CheckRef,
                          override val timestamp: DateTime,
                          correlationId: UUID,
                          acknowledgementId: UUID)
extends CheckNotification(checkRef, timestamp, "check-recovers", "check health recovers", Some(correlationId)) with Alert

/**
 *
 */
case class NotifyLifecycleChanges(override val checkRef: CheckRef,
                                  override val timestamp: DateTime,
                                  oldLifecycle: CheckLifecycle,
                                  newLifecycle: CheckLifecycle)
extends CheckNotification(checkRef, timestamp, "lifecycle-changes", "check lifecycle transitions from %s to %s".format(oldLifecycle.toString, newLifecycle.toString), None)

/**
 *
 */
case class NotifyHealthChanges(override val checkRef: CheckRef,
                               override val timestamp: DateTime,
                               override val correlation: Option[UUID],
                               oldHealth: CheckHealth,
                               newHealth: CheckHealth)
extends CheckNotification(checkRef, timestamp, "health-changes", "check transitions from %s to %s".format(oldHealth.toString, newHealth.toString), correlation)

/**
 *
 */
case class NotifyHealthUpdates(override val checkRef: CheckRef,
                               override val timestamp: DateTime,
                               override val correlation: Option[UUID],
                               health: CheckHealth)
extends CheckNotification(checkRef, timestamp, "health-updates", "check is " + health.toString, correlation)

/**
 *
 */
case class NotifyHealthExpires(override val checkRef: CheckRef,
                               override val timestamp: DateTime,
                               override val correlation: Option[UUID])
extends CheckNotification(checkRef, timestamp, "health-expires", "check health expires", correlation)

/**
 *
 */
case class NotifyHealthFlaps(override val checkRef: CheckRef,
                             override val timestamp: DateTime,
                             override val correlation: Option[UUID],
                             flapStarts: DateTime)
extends CheckNotification(checkRef, timestamp, "health-flaps", "check health is flapping", correlation)


/**
 *
 */
case class NotifySquelched(override val checkRef: CheckRef,
                           override val timestamp: DateTime)
extends CheckNotification(checkRef, timestamp, "check-squelched", "check notifications disabled", None)

/**
 *
 */
case class NotifyUnsquelched(override val checkRef: CheckRef,
                             override val timestamp: DateTime)
extends CheckNotification(checkRef, timestamp, "check-unsquelched", "check notifications enabled", None)

/**
 *
 */
case class Contact(id: String, name: String, metadata: Map[String,String]) extends NotificationModel

/**
 *
 */
case class ContactGroup(id: String, name: String, metadata: Map[String,String], contacts: Set[Contact]) extends NotificationModel

/**
 *
 */
case class NotifyContact(contact: Contact, notification: NotificationEvent) extends NotificationModel
