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

  // config
  val persister: ActorRef = {
    val props = ServiceExtension.makePluginProps(settings.persister.plugin, settings.persister.settings)
    log.info("loading persister plugin {}", settings.persister.plugin)
    context.actorOf(props, "persister")
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
  }
}

object StateManager {
  def props(settings: StateSettings) = Props(classOf[StateManager], settings)
}

case class ProbeCondition(timestamp: DateTime,
                          lifecycle: ProbeLifecycle,
                          summary: Option[String],
                          health: ProbeHealth,
                          correlation: Option[UUID],
                          acknowledged: Option[UUID],
                          squelched: Boolean)


case class ProbeNotifications(notifications: Vector[ProbeNotification])

case class ProbeMetrics(metrics: Map[String,BigDecimal])

/**
 *
 */
sealed trait StateServiceOperation extends ServiceOperation
sealed trait StateServiceCommand extends ServiceCommand with StateServiceOperation
sealed trait StateServiceQuery extends ServiceQuery with StateServiceOperation
case class StateServiceOperationFailed(op: StateServiceOperation, failure: Throwable) extends ServiceOperationFailed

case class InitializeProbeStatus(probeRef: ProbeRef, timestamp: DateTime) extends StateServiceCommand
case class InitializeProbeStatusResult(op: InitializeProbeStatus, status: Option[ProbeStatus])

case class UpdateProbeStatus(probeRef: ProbeRef, status: ProbeStatus, notifications: Vector[ProbeNotification], lastTimestamp: Option[DateTime]) extends StateServiceCommand
case class UpdateProbeStatusResult(op: UpdateProbeStatus)

case class DeleteProbeStatus(probeRef: ProbeRef, lastStatus: Option[ProbeStatus]) extends StateServiceCommand
case class DeleteProbeStatusResult(op: DeleteProbeStatus)

case class TrimProbeHistory(probeRef: ProbeRef, until: DateTime) extends StateServiceCommand
case class TrimProbeHistoryResult(op: TrimProbeHistory)

case class GetConditionHistory(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends StateServiceQuery
case class GetConditionHistoryResult(op: GetConditionHistory, history: Vector[ProbeCondition])

case class GetNotificationHistory(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends StateServiceQuery
case class GetNotificationHistoryResult(op: GetNotificationHistory, history: Vector[ProbeNotification])

//case class GetMetricHistory(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime], limit: Option[Int]) extends StateServiceQuery
//case class GetMetricHistoryResult(op: GetMetricHistory, history: Vector[ProbeMetric])

/* marker trait for Persister implementations */
trait Persister
