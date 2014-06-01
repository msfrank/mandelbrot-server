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

import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import org.joda.time.DateTime
import java.util.UUID

import io.mandelbrot.core.{ResourceNotFound, ApiException, ServiceExtension, ServerConfig}
import io.mandelbrot.core.history.HistoryService
import io.mandelbrot.core.registry.ProbeMatcher
import scala.collection.mutable

/**
 *
 */
class NotificationManager extends Actor with ActorLogging {

  // config
  val settings = ServerConfig(context.system).settings.notification
  val notifiers: Map[String,ActorRef] = settings.notifiers.map { case (name, notifierSettings) =>
    val props = ServiceExtension.makePluginProps(notifierSettings.plugin, notifierSettings.settings)
    log.info("loading notifier plugin {}", name)
    name -> context.actorOf(props, name)
  }

  // state
  val windows = new mutable.HashMap[UUID,MaintenanceWindow]()

  val historyService = HistoryService(context.system)

  if (settings.rules.rules.isEmpty)
    log.debug("notification rules set is empty")
  else
    log.debug("using notification rules:\n{}", settings.rules.rules.map("    " + _).mkString("\n"))

  def receive = {

    case command: RegisterMaintenanceWindow =>
      val id = UUID.randomUUID()
      val window = MaintenanceWindow(id, command.affected, command.from, command.to)
      windows.put(id, window)
      log.debug("registered window {}", window)
      sender() ! RegisterMaintenanceWindowResult(command, id)

    case command: UnregisterMaintenanceWindow =>
      windows.remove(command.id) match {
        case Some(window) =>
          log.debug("unregistered window {}", window)
          sender() ! UnregisterMaintenanceWindowResult(command, command.id)
        case None =>
          sender() ! NotificationManagerOperationFailed(command, new ApiException(ResourceNotFound))
      }

    case query: ListMaintenanceWindows =>
      sender() ! ListMaintenanceWindowsResult(query, windows.values.toVector)

    case notification: Notification =>
      if (!isSuppressed(notification)) {
        settings.rules.evaluate(notification, notifiers)
        historyService ! notification
      }
  }

  /**
   *
   */
  def isSuppressed(notification: Notification): Boolean = notification match {
    case probeNotification: ProbeNotification =>
      windows.values.foreach {
        case window if notification.timestamp.isAfter(window.from) && notification.timestamp.isBefore(window.to) =>
          window.affected.foreach{ matcher => if (matcher.matches(probeNotification.probeRef)) return true }
        case _ => // do nothing
      }
      false
    case _ => false
  }
}

object NotificationManager {
  def props() = Props(classOf[NotificationManager])
}

/* */
case class MaintenanceWindow(id: UUID, affected: Vector[ProbeMatcher], from: DateTime, to: DateTime)
case class MaintenanceStats(id: UUID, numSuppressed: Long)

/* notification manager operations */
sealed trait NotificationManagerOperation
sealed trait NotificationManagerQuery extends NotificationManagerOperation
sealed trait NotificationManagerCommand extends NotificationManagerOperation
case class NotificationManagerOperationFailed(op: NotificationManagerOperation, failure: Throwable)

case class RegisterMaintenanceWindow(affected: Vector[ProbeMatcher], from: DateTime, to: DateTime) extends NotificationManagerCommand
case class RegisterMaintenanceWindowResult(op: RegisterMaintenanceWindow, id: UUID)

case class ListMaintenanceWindows() extends NotificationManagerQuery
case class ListMaintenanceWindowsResult(op: ListMaintenanceWindows, windows: Vector[MaintenanceWindow])

case class UnregisterMaintenanceWindow(id: UUID) extends NotificationManagerCommand
case class UnregisterMaintenanceWindowResult(op: UnregisterMaintenanceWindow, id: UUID)


/* marker trait for Notifier implementations */
trait Notifier