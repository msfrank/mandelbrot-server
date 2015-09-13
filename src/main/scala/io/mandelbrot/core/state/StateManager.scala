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
import io.mandelbrot.core.model._

/**
 *
 */
class StateManager(settings: StateSettings, clusterEnabled: Boolean) extends Actor with ActorLogging {

  // config
  val persister: ActorRef = context.actorOf(settings.props, "persister")

  def receive = {

    /* append observation to the specified probe */
    case op: AppendObservation =>
      persister forward op

    /* retrieve the current status for the specified check */
    case op: GetStatus =>
      persister forward op

    /* append status to the specified check */
    case op: UpdateStatus =>
      persister forward op

    /* delete state for the specified check */
    case op: DeleteStatus =>
      persister forward op

    /* retrieve probe observation history */
    case op: GetObservationHistory =>
      persister forward op

    /* retrieve check condition history */
    case op: GetConditionHistory =>
      persister forward op

    /* retrieve check notifications history */
    case op: GetNotificationsHistory =>
      persister forward op

    case unhandled =>
      log.error("dropping unhandled message {}", unhandled)
  }
}

object StateManager {
  def props(settings: StateSettings, clusterEnabled: Boolean) = {
    Props(classOf[StateManager], settings, clusterEnabled)
  }
}

/**
 *
 */
sealed trait StateServiceOperation extends ServiceOperation
sealed trait StateServiceCommand extends ServiceCommand with StateServiceOperation
sealed trait StateServiceQuery extends ServiceQuery with StateServiceOperation
sealed trait StateServiceResult
case class StateServiceOperationFailed(op: StateServiceOperation, failure: Throwable) extends ServiceOperationFailed

case class AppendObservation(probeRef: ProbeRef,
                             generation: Long,
                             observation: Observation,
                             commitEpoch: Boolean = false) extends StateServiceCommand
case class AppendObservationResult(op: AppendObservation) extends StateServiceResult

case class UpdateStatus(checkRef: CheckRef,
                        status: CheckStatus,
                        notifications: Vector[CheckNotification],
                        commitEpoch: Boolean = false) extends StateServiceCommand
case class UpdateStatusResult(op: UpdateStatus) extends StateServiceResult

case class DeleteStatus(checkRef: CheckRef, generation: Long) extends StateServiceCommand
case class DeleteStatusResult(op: DeleteStatus) extends StateServiceResult

case class GetStatus(checkRef: CheckRef, generation: Long) extends StateServiceQuery
case class GetStatusResult(op: GetStatus, status: Option[CheckStatus]) extends StateServiceResult

case class GetObservationHistory(probeRef: ProbeRef,
                                 generation: Long,
                                 from: Option[DateTime],
                                 to: Option[DateTime],
                                 limit: Int,
                                 fromInclusive: Boolean = false,
                                 toExclusive: Boolean = false,
                                 descending: Boolean = false,
                                 last: Option[String] = None) extends StateServiceQuery
case class GetObservationHistoryResult(op: GetObservationHistory, page: ProbeObservationPage) extends StateServiceResult

case class GetConditionHistory(checkRef: CheckRef,
                               generation: Long,
                               from: Option[DateTime],
                               to: Option[DateTime],
                               limit: Int,
                               fromInclusive: Boolean = false,
                               toExclusive: Boolean = false,
                               descending: Boolean = false,
                               last: Option[String] = None) extends StateServiceQuery
case class GetConditionHistoryResult(op: GetConditionHistory, page: CheckConditionPage) extends StateServiceResult

case class GetNotificationsHistory(checkRef: CheckRef,
                                   generation: Long,
                                   from: Option[DateTime],
                                   to: Option[DateTime],
                                   limit: Int,
                                   fromInclusive: Boolean = false,
                                   toExclusive: Boolean = false,
                                   descending: Boolean = false,
                                   last: Option[String] = None) extends StateServiceQuery
case class GetNotificationsHistoryResult(op: GetNotificationsHistory, page: CheckNotificationsPage) extends StateServiceResult
