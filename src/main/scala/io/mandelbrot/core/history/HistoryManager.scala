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

package io.mandelbrot.core.history

import akka.actor._
import io.mandelbrot.core.system.ProbeRef
import org.joda.time.{DateTimeZone, DateTime}

import io.mandelbrot.core._
import io.mandelbrot.core.notification.ProbeNotification
import io.mandelbrot.core.system.ProbeStatus

/**
 *
 */
class HistoryManager(settings: HistorySettings) extends Actor with ActorLogging {
  import HistoryManager._
  import context.dispatcher

  // state
  val archiver: ActorRef = {
    val props = ServiceExtension.makePluginProps(settings.archiver.plugin, settings.archiver.settings)
    log.info("loading archiver plugin {}", settings.archiver.plugin)
    context.actorOf(props, "archiver")
  }
  var historyCleaner: Option[Cancellable] = None

  override def preStart(): Unit = {
    historyCleaner = Some(context.system.scheduler.schedule(settings.cleanerInitialDelay, settings.cleanerInterval, self, RunCleaner))
  }

  override def postStop(): Unit = {
    for (cancellable <- historyCleaner)
      cancellable.cancel()
    historyCleaner = None
  }

  def receive = {

    /* append probe status to history */
    case StatusAppends(status) =>
      archiver ! status

    /* append notification to history */
    case NotificationAppends(notification) =>
      archiver ! notification

    /* retrieve status history */
    case query: GetStatusHistory =>
      archiver.forward(query)

    /* retrieve notification history */
    case query: GetNotificationHistory =>
      archiver.forward(query)

    case RunCleaner =>
      val mark = new DateTime(DateTime.now(DateTimeZone.UTC).getMillis - settings.historyRetention.toMillis)
      archiver ! CleanHistory(mark)
  }
}

object HistoryManager {
  def props(settings: HistorySettings) = Props(classOf[HistoryManager], settings)
  case object RunCleaner
}

case class CleanHistory(mark: DateTime)

/* */
sealed trait HistoryServiceOperation extends ServiceOperation
sealed trait HistoryServiceCommand extends ServiceCommand with HistoryServiceOperation
sealed trait HistoryServiceQuery extends ServiceQuery with HistoryServiceOperation
sealed trait HistoryServiceEvent extends ServiceEvent with HistoryServiceOperation
case class HistoryServiceOperationFailed(op: HistoryServiceOperation, failure: Throwable) extends ServiceOperationFailed

case class GetStatusHistory(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends HistoryServiceQuery
case class GetStatusHistoryResult(op: GetStatusHistory, history: Vector[ProbeStatus])

case class GetNotificationHistory(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends HistoryServiceQuery
case class GetNotificationHistoryResult(op: GetNotificationHistory, history: Vector[ProbeNotification])

case class DeleteAllHistory(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime]) extends HistoryServiceCommand
case class DeleteHistoryFor(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime]) extends HistoryServiceCommand

case class StatusAppends(status: ProbeStatus) extends HistoryServiceEvent
case class NotificationAppends(notification: ProbeNotification) extends HistoryServiceEvent

/* marker trait for Archiver implementations */
trait Archiver
