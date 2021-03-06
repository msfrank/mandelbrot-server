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

import akka.actor._
import org.joda.time.DateTime
import scala.collection.mutable
import java.util.UUID

import io.mandelbrot.core._
import io.mandelbrot.core.model._

/**
 * the notification manager holds notification routing configuration as well as
 * the state for scheduled maintenance windows.  this actor receives notifications
 * from Checks and routes them to the appropriate Notifier instances, depending on
 * current maintenance windows and notification rules.
 */
class NotificationManager(settings: NotificationSettings, clusterEnabled: Boolean) extends Actor with ActorLogging {
  import NotificationManager._
  import context.dispatcher

  // config
  val notifiers: Map[String,ActorRef] = settings.notifiers.map {
    case (name, NotifierSettings(plugin, props)) => name -> context.actorOf(props, name)
  }
  val historyService = context.parent

  // state
  val windows = new mutable.HashMap[UUID,MaintenanceWindow]()

  def receive = {

    case query: ListNotificationRules =>
      sender() ! NotificationServiceOperationFailed(query, ApiException(NotImplemented))

    case notification: CheckNotification =>
      if (!isSuppressed(notification)) {
        settings.rules.foreach(_.evaluate(notification, notifiers))
      }

    case notification: NotificationEvent =>
      if (!isSuppressed(notification)) {
        settings.rules.foreach(_.evaluate(notification, notifiers))
      }

  }

  /**
   *
   */
  def isSuppressed(notification: NotificationEvent): Boolean = notification match {
    case checkNotification: CheckNotification =>
      windows.values.foreach {
        case window if notification.timestamp.isAfter(window.from) && notification.timestamp.isBefore(window.to) =>
          window.affected.foreach { matcher => if (matcher.matches(checkNotification.checkRef.checkId)) return true }
        case _ => // do nothing
      }
      false
    case _ => false
  }
}

object NotificationManager {
  def props(settings: NotificationSettings, clusterEnabled: Boolean) = {
    Props(classOf[NotificationManager], settings, clusterEnabled)
  }

  sealed trait Event
  case class MaintenanceWindowRegisters(command: RegisterMaintenanceWindow, window: MaintenanceWindow) extends Event
  case class MaintenanceWindowUpdates(command: ModifyMaintenanceWindow) extends Event
  case class MaintenanceWindowUnregisters(command: UnregisterMaintenanceWindow) extends Event
  case class MaintenanceWindowExpires(id: UUID) extends Event
  case class NotificationManagerSnapshot(windows: Map[UUID,MaintenanceWindow]) extends Serializable

  case object RunCleaner
  case object TakeSnapshot
}


/* */
case class MaintenanceWindow(id: UUID, affected: Set[CheckMatcher], from: DateTime, to: DateTime, description: Option[String])
case class MaintenanceStats(id: UUID, numSuppressed: Long)
case class MaintenanceWindowModification(added: Option[Set[CheckMatcher]],
                                         removed: Option[Set[CheckMatcher]],
                                         from: Option[DateTime],
                                         to: Option[DateTime],
                                         description: Option[String])

/* notification manager operations */
trait NotificationServiceOperation extends ServiceOperation
sealed trait NotificationServiceQuery extends ServiceCommand with NotificationServiceOperation
sealed trait NotificationServiceCommand extends ServiceQuery with NotificationServiceOperation
case class NotificationServiceOperationFailed(op: NotificationServiceOperation, failure: Throwable) extends ServiceOperationFailed

case class ListNotificationRules() extends NotificationServiceQuery
case class ListNotificationRulesResult(op: ListNotificationRules, rules: Vector[NotificationRule])

case class ModifyMaintenanceWindow(id: UUID, modifications: MaintenanceWindowModification) extends NotificationServiceCommand
case class ModifyMaintenanceWindowResult(op: ModifyMaintenanceWindow, id: UUID)

case class RegisterMaintenanceWindow(affected: Set[CheckMatcher], from: DateTime, to: DateTime, description: Option[String]) extends NotificationServiceCommand
case class RegisterMaintenanceWindowResult(op: RegisterMaintenanceWindow, id: UUID)

case class ListMaintenanceWindows() extends NotificationServiceQuery
case class ListMaintenanceWindowsResult(op: ListMaintenanceWindows, windows: Vector[MaintenanceWindow])

case class UnregisterMaintenanceWindow(id: UUID) extends NotificationServiceCommand
case class UnregisterMaintenanceWindowResult(op: UnregisterMaintenanceWindow, id: UUID)
