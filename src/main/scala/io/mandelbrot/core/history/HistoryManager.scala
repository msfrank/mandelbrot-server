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
import org.joda.time.{DateTimeZone, DateTime}

import io.mandelbrot.core.registry._
import io.mandelbrot.core.{ServiceExtension, ServerConfig}
import io.mandelbrot.core.notification.ProbeNotification
import io.mandelbrot.core.registry.ProbeStatus

/**
 *
 */
class HistoryManager extends Actor with ActorLogging {
  import HistoryManager._
  import context.dispatcher

  // config
  val settings = ServerConfig(context.system).settings.history
  val archiver: ActorRef = {
    val props = ServiceExtension.makePluginProps(settings.archiver.plugin, settings.archiver.settings)
    log.info("loading archiver plugin {}", settings.archiver.plugin)
    context.actorOf(props, "archiver")
  }

  // state
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
    case status: ProbeStatus =>
      archiver ! status

    /* append notification to history */
    case notification: ProbeNotification =>
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
  def props() = Props(classOf[HistoryManager])
  case object RunCleaner
}

case class CleanHistory(mark: DateTime)

/* */
sealed trait HistoryServiceOperation
sealed trait HistoryServiceCommand extends HistoryServiceOperation
sealed trait HistoryServiceQuery extends HistoryServiceOperation
case class HistoryServiceOperationFailed(op: HistoryServiceOperation, failure: Throwable)

case class GetStatusHistory(refspec: Either[ProbeRef,Set[ProbeRef]], from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends HistoryServiceQuery
case class GetStatusHistoryResult(op: GetStatusHistory, history: Vector[ProbeStatus])

case class GetNotificationHistory(refspec: Either[ProbeRef,Set[ProbeRef]], from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends HistoryServiceQuery
case class GetNotificationHistoryResult(op: GetNotificationHistory, history: Vector[ProbeNotification])

case class DeleteAllHistory(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime]) extends HistoryServiceCommand
case class DeleteHistoryFor(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime]) extends HistoryServiceCommand

/* marker trait for Archiver implementations */
trait Archiver
