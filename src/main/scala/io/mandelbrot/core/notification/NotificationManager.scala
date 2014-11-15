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
import io.mandelbrot.core.history.NotificationAppends
import io.mandelbrot.core.system.ProbeMatcher
import org.joda.time.DateTime
import scala.collection.mutable
import java.util.UUID

import io.mandelbrot.core._

/**
 * the notification manager holds notification routing configuration as well as
 * the state for scheduled maintenance windows.  this actor receives notifications
 * from Probes and routes them to the appropriate Notifier instances, depending on
 * current maintenance windows and notification rules.
 */
class NotificationManager extends Actor with ActorLogging {
  import NotificationManager._
  import context.dispatcher

  // config
  val settings = ServerConfig(context.system).settings.notification
  val notifiers: Map[String,ActorRef] = settings.notifiers.map { case (name, notifierSettings) =>
    val props = ServiceExtension.makePluginProps(notifierSettings.plugin, notifierSettings.settings)
    log.info("loading notifier plugin {}", name)
    name -> context.actorOf(props, name)
  }
  val historyService = context.parent

  // state
  val windows = new mutable.HashMap[UUID,MaintenanceWindow]()

  def receive = {

    case query: ListNotificationRules =>
      sender() ! ListNotificationRulesResult(query, settings.rules.rules)

    case notification: ProbeNotification =>
      if (!isSuppressed(notification)) {
        settings.rules.evaluate(notification, notifiers)
        historyService ! NotificationAppends(notification)
      }

    case notification: NotificationEvent =>
      if (!isSuppressed(notification)) {
        settings.rules.evaluate(notification, notifiers)
      }

  }

  /**
   *
   */
  def isSuppressed(notification: NotificationEvent): Boolean = notification match {
    case probeNotification: ProbeNotification =>
      windows.values.foreach {
        case window if notification.timestamp.isAfter(window.from) && notification.timestamp.isBefore(window.to) =>
          window.affected.foreach { matcher => if (matcher.matches(probeNotification.probeRef)) return true }
        case _ => // do nothing
      }
      false
    case _ => false
  }
}

object NotificationManager {
  def props() = Props(classOf[NotificationManager])

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
case class MaintenanceWindow(id: UUID, affected: Set[ProbeMatcher], from: DateTime, to: DateTime, description: Option[String])
case class MaintenanceStats(id: UUID, numSuppressed: Long)
case class MaintenanceWindowModification(added: Option[Set[ProbeMatcher]],
                                         removed: Option[Set[ProbeMatcher]],
                                         from: Option[DateTime],
                                         to: Option[DateTime],
                                         description: Option[String])

/* notification manager operations */
trait NotificationServiceOperation
sealed trait NotificationServiceQuery extends NotificationServiceOperation
sealed trait NotificationServiceCommand extends NotificationServiceOperation
case class NotificationServiceOperationFailed(op: NotificationServiceOperation, failure: Throwable)

case class ListNotificationRules() extends NotificationServiceQuery
case class ListNotificationRulesResult(op: ListNotificationRules, rules: Vector[NotificationRule])

case class ModifyMaintenanceWindow(id: UUID, modifications: MaintenanceWindowModification) extends NotificationServiceCommand
case class ModifyMaintenanceWindowResult(op: ModifyMaintenanceWindow, id: UUID)

case class RegisterMaintenanceWindow(affected: Set[ProbeMatcher], from: DateTime, to: DateTime, description: Option[String]) extends NotificationServiceCommand
case class RegisterMaintenanceWindowResult(op: RegisterMaintenanceWindow, id: UUID)

case class ListMaintenanceWindows() extends NotificationServiceQuery
case class ListMaintenanceWindowsResult(op: ListMaintenanceWindows, windows: Vector[MaintenanceWindow])

case class UnregisterMaintenanceWindow(id: UUID) extends NotificationServiceCommand
case class UnregisterMaintenanceWindowResult(op: UnregisterMaintenanceWindow, id: UUID)


/* marker trait for Notifier implementations */
trait Notifier