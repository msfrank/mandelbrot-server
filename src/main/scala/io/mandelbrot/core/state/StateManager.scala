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
class StateManager(settings: StateSettings) extends Actor with ActorLogging {

  // config
  val persister: ActorRef = context.actorOf(settings.props, "persister")

  def receive = {

    /* retrieve the current status for the specified check */
    case op: InitializeCheckStatus =>
      persister forward op

    /* append status to the specified check */
    case op: UpdateCheckStatus =>
      persister forward op

    /* delete state for the specified check */
    case op: DeleteCheckStatus =>
      persister forward op

    /* retrieve condition history */
    case op: GetConditionHistory =>
      persister forward op

    /* retrieve notifications history */
    case op: GetNotificationsHistory =>
      persister forward op

    /* retrieve metrics history */
    case op: GetMetricsHistory =>
      persister forward op
  }
}

object StateManager {
  def props(settings: StateSettings) = Props(classOf[StateManager], settings)
}

/**
 *
 */
sealed trait StateServiceOperation extends ServiceOperation
sealed trait StateServiceCommand extends ServiceCommand with StateServiceOperation
sealed trait StateServiceQuery extends ServiceQuery with StateServiceOperation
sealed trait StateServiceResult
case class StateServiceOperationFailed(op: StateServiceOperation, failure: Throwable) extends ServiceOperationFailed

case class InitializeCheckStatus(checkRef: CheckRef, timestamp: DateTime) extends StateServiceCommand
case class InitializeCheckStatusResult(op: InitializeCheckStatus, status: Option[CheckStatus]) extends StateServiceResult

case class UpdateCheckStatus(checkRef: CheckRef, status: CheckStatus, notifications: Vector[CheckNotification], lastTimestamp: Option[DateTime]) extends StateServiceCommand
case class UpdateCheckStatusResult(op: UpdateCheckStatus) extends StateServiceResult

case class DeleteCheckStatus(checkRef: CheckRef, until: Option[DateTime]) extends StateServiceCommand
case class DeleteCheckStatusResult(op: DeleteCheckStatus) extends StateServiceResult

case class GetConditionHistory(checkRef: CheckRef, from: Option[DateTime], to: Option[DateTime], limit: Int, last: Option[String]) extends StateServiceQuery
case class GetConditionHistoryResult(op: GetConditionHistory, page: CheckConditionPage) extends StateServiceResult

case class GetNotificationsHistory(checkRef: CheckRef, from: Option[DateTime], to: Option[DateTime], limit: Int, last: Option[String]) extends StateServiceQuery
case class GetNotificationsHistoryResult(op: GetNotificationsHistory, page: CheckNotificationsPage) extends StateServiceResult

case class GetMetricsHistory(checkRef: CheckRef, from: Option[DateTime], to: Option[DateTime], limit: Int, last: Option[String]) extends StateServiceQuery
case class GetMetricsHistoryResult(op: GetMetricsHistory, page: CheckMetricsPage) extends StateServiceResult
