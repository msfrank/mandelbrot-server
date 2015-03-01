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

package io.mandelbrot.core.state

import akka.actor._
import org.joda.time.DateTime
import java.util.UUID

import io.mandelbrot.core._
import io.mandelbrot.core.system._
import io.mandelbrot.core.notification.ProbeNotification

/**
 *
 */
class StateManager(settings: StateSettings) extends Actor with ActorLogging {
  import StateManager._

  // config
  val persister: ActorRef = {
    val props = ServiceExtension.makePluginProps(settings.persister.plugin, settings.persister.settings)
    log.info("loading persister plugin {}", settings.persister.plugin)
    context.actorOf(props, "persister")
  }
  var historyCleaner: Option[Cancellable] = None

  override def preStart(): Unit = {
    //historyCleaner = Some(context.system.scheduler.schedule(settings.cleanerInitialDelay, settings.cleanerInterval, self, PerformTrim))
  }

  override def postStop(): Unit = {
    for (cancellable <- historyCleaner)
      cancellable.cancel()
    historyCleaner = None
  }
  
  def receive = {

    case op: InitializeProbeStatus =>
      persister forward op
      
    case op: UpdateProbeStatus =>
      persister forward op
      
    case op: DeleteProbeStatus =>
      persister forward op

    /* retrieve condition history */
    case op: GetConditionHistory =>
      persister forward op

    /* retrieve notification history */
    case op: GetNotificationHistory =>
      persister forward op

//    /* retrieve metric history */
//    case op: GetMetricHistory =>
//      persister forward op

    case PerformTrim =>
      //val mark = new DateTime(DateTime.now(DateTimeZone.UTC).getMillis - settings.historyRetention.toMillis)
      //persister ! TrimProbeHistory(mark)
  }
}

object StateManager {
  def props(settings: StateSettings) = Props(classOf[StateManager], settings)
  
  case object PerformTrim
}

case class ProbeCondition(timestamp: DateTime,
                          lifecycle: ProbeLifecycle,
                          summary: Option[String],
                          health: ProbeHealth,
                          correlation: Option[UUID],
                          acknowledged: Option[UUID],
                          squelched: Boolean)

/**
 *
 */
sealed trait StateServiceOperation extends ServiceOperation
sealed trait StateServiceCommand extends ServiceCommand with StateServiceOperation
sealed trait StateServiceQuery extends ServiceQuery with StateServiceOperation
case class StateServiceOperationFailed(op: StateServiceOperation, failure: Throwable) extends ServiceOperationFailed

case class InitializeProbeStatus(probeRef: ProbeRef, timestamp: DateTime) extends StateServiceCommand
case class InitializeProbeStatusResult(op: InitializeProbeStatus, status: ProbeStatus, seqNum: Long)

case class UpdateProbeStatus(probeRef: ProbeRef, status: ProbeStatus, notifications: Vector[ProbeNotification], seqNum: Long) extends StateServiceCommand
case class UpdateProbeStatusResult(op: UpdateProbeStatus)

case class DeleteProbeStatus(probeRef: ProbeRef, lastStatus: Option[ProbeStatus], seqNum: Long) extends StateServiceCommand
case class DeleteProbeStatusResult(op: DeleteProbeStatus)

case class TrimProbeHistory(probeRef: ProbeRef, mark: DateTime) extends StateServiceCommand
case class TrimProbeHistoryResult(op: TrimProbeHistory)

case class GetConditionHistory(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends StateServiceQuery
case class GetConditionHistoryResult(op: GetConditionHistory, history: Vector[ProbeCondition])

case class GetNotificationHistory(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends StateServiceQuery
case class GetNotificationHistoryResult(op: GetNotificationHistory, history: Vector[ProbeNotification])

//case class GetMetricHistory(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends StateServiceQuery
//case class GetMetricHistoryResult(op: GetMetricHistory, history: Vector[ProbeMetric])

/* marker trait for Persister implementations */
trait Persister
