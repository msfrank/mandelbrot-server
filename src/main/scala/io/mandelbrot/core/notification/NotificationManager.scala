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
import akka.persistence.{SnapshotOffer, SaveSnapshotSuccess, SaveSnapshotFailure, EventsourcedProcessor}
import org.joda.time.DateTime
import scala.collection.mutable
import java.util.UUID

import io.mandelbrot.core.{ResourceNotFound, ApiException, ServiceExtension, ServerConfig}
import io.mandelbrot.core.history.HistoryService
import io.mandelbrot.core.registry.ProbeMatcher

/**
 * the notification manager holds notification routing configuration as well as
 * the state for scheduled maintenance windows.  this actor receives notifications
 * from Probes and routes them to the appropriate Notifier instances, depending on
 * current maintenance windows and notification rules.
 */
class NotificationManager extends EventsourcedProcessor with ActorLogging {
  import NotificationManager._
  import context.dispatcher

  // config
  override def processorId = "notification-manager"
  val settings = ServerConfig(context.system).settings.notification
  val notifiers: Map[String,ActorRef] = settings.notifiers.map { case (name, notifierSettings) =>
    val props = ServiceExtension.makePluginProps(notifierSettings.plugin, notifierSettings.settings)
    log.info("loading notifier plugin {}", name)
    name -> context.actorOf(props, name)
  }

  // state
  val windows = new mutable.HashMap[UUID,MaintenanceWindow]()
  var snapshotCancellable: Option[Cancellable] = None

  // refs
  val historyService = HistoryService(context.system)


  override def preStart(): Unit = {
    super.preStart()
    // schedule regular snapshots
    snapshotCancellable = Some(context.system.scheduler.schedule(settings.snapshotInitialDelay, settings.snapshotInterval, self, TakeSnapshot))
    log.debug("scheduling {} snapshots every {} with initial delay of {}",
      processorId, settings.snapshotInterval.toString(), settings.snapshotInitialDelay.toString())
  }

  override def postStop(): Unit = {
    for (cancellable <- snapshotCancellable)
      cancellable.cancel()
    super.postStop()
  }

  def receiveCommand = {

    case query: ListNotificationRules =>
      sender() ! ListNotificationRulesResult(query, settings.rules.rules)

    case command: RegisterMaintenanceWindow =>
      val id = UUID.randomUUID()
      val window = MaintenanceWindow(id, command.affected, command.from, command.to)
      persist(MaintenanceWindowRegisters(command, window))(updateState)

    case command: UnregisterMaintenanceWindow =>
      if (windows.contains(command.id))
        persist(MaintenanceWindowUnregisters(command))(updateState)
      else
        sender() ! NotificationManagerOperationFailed(command, new ApiException(ResourceNotFound))

    case query: ListMaintenanceWindows =>
      sender() ! ListMaintenanceWindowsResult(query, windows.values.toVector)

    case notification: Notification =>
      if (!isSuppressed(notification)) {
        settings.rules.evaluate(notification, notifiers)
        historyService ! notification
      }

    /* */
    case TakeSnapshot =>
      log.debug("snapshotting {}, last sequence number is {}", processorId, lastSequenceNr)
      saveSnapshot(NotificationManagerSnapshot(windows.toMap))

    case SaveSnapshotSuccess(metadata) =>
      log.debug("saved snapshot successfully: {}", metadata)

    case SaveSnapshotFailure(metadata, cause) =>
      log.warning("failed to save snapshot {}: {}", metadata, cause.getMessage)
  }

  def receiveRecover = {

    case event: Event =>
      updateState(event)

    /* recreate probe state from snapshot */
    case SnapshotOffer(metadata, snapshot: NotificationManagerSnapshot) =>
      log.debug("loading snapshot of {} using offer {}", processorId, metadata)
      snapshot.windows.foreach { case (id,window) =>
        windows.put(id, window)
      }
  }

  def updateState(event: Event): Unit = event match {

    case MaintenanceWindowRegisters(command, window) =>
      windows.put(window.id, window)
      log.debug("registered maintenance window {}", window)
      if (!recoveryRunning)
        sender() ! RegisterMaintenanceWindowResult(command, window.id)

    case MaintenanceWindowUnregisters(command) =>
      windows.remove(command.id)
      log.debug("unregistered maintenance window {}", command.id)
      if (!recoveryRunning)
        sender() ! UnregisterMaintenanceWindowResult(command, command.id)

    case MaintenanceWindowExpires(id) =>
      windows.remove(id)
      log.debug("maintenance window {} expires", id)
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
  sealed trait Event
  case class MaintenanceWindowRegisters(command: RegisterMaintenanceWindow, window: MaintenanceWindow) extends Event
  case class MaintenanceWindowUnregisters(command: UnregisterMaintenanceWindow) extends Event
  case class MaintenanceWindowExpires(id: UUID) extends Event
  case class NotificationManagerSnapshot(windows: Map[UUID,MaintenanceWindow]) extends Serializable

  case object TakeSnapshot
}

/* */
case class MaintenanceWindow(id: UUID, affected: Vector[ProbeMatcher], from: DateTime, to: DateTime)
case class MaintenanceStats(id: UUID, numSuppressed: Long)

/* notification manager operations */
sealed trait NotificationManagerOperation
sealed trait NotificationManagerQuery extends NotificationManagerOperation
sealed trait NotificationManagerCommand extends NotificationManagerOperation
case class NotificationManagerOperationFailed(op: NotificationManagerOperation, failure: Throwable)

case class ListNotificationRules() extends NotificationManagerQuery
case class ListNotificationRulesResult(op: ListNotificationRules, rules: Vector[NotificationRule])

case class RegisterMaintenanceWindow(affected: Vector[ProbeMatcher], from: DateTime, to: DateTime) extends NotificationManagerCommand
case class RegisterMaintenanceWindowResult(op: RegisterMaintenanceWindow, id: UUID)

case class ListMaintenanceWindows() extends NotificationManagerQuery
case class ListMaintenanceWindowsResult(op: ListMaintenanceWindows, windows: Vector[MaintenanceWindow])

case class UnregisterMaintenanceWindow(id: UUID) extends NotificationManagerCommand
case class UnregisterMaintenanceWindowResult(op: UnregisterMaintenanceWindow, id: UUID)


/* marker trait for Notifier implementations */
trait Notifier