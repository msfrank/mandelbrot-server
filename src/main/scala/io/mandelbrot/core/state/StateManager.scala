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

    case op: InitializeCheckStatus =>
      persister forward op
      
    case op: UpdateCheckStatus =>
      persister forward op
      
    case op: DeleteCheckStatus =>
      persister forward op

    /* retrieve condition history */
    case op: GetConditionHistory =>
      persister forward op

    /* retrieve notification history */
    case op: GetNotificationHistory =>
      persister forward op

    /* retrieve metric history */
    case op: GetMetricHistory =>
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

case class DeleteCheckStatus(checkRef: CheckRef, lastStatus: Option[CheckStatus]) extends StateServiceCommand
case class DeleteCheckStatusResult(op: DeleteCheckStatus) extends StateServiceResult

case class TrimCheckHistory(checkRef: CheckRef, until: DateTime) extends StateServiceCommand
case class TrimCheckHistoryResult(op: TrimCheckHistory) extends StateServiceResult

case class GetConditionHistory(checkRef: CheckRef, from: Option[DateTime], to: Option[DateTime], limit: Int, last: Option[DateTime]) extends StateServiceQuery
case class GetConditionHistoryResult(op: GetConditionHistory, page: CheckConditionPage) extends StateServiceResult

case class GetNotificationHistory(checkRef: CheckRef, from: Option[DateTime], to: Option[DateTime], limit: Int, last: Option[DateTime]) extends StateServiceQuery
case class GetNotificationHistoryResult(op: GetNotificationHistory, page: CheckNotificationsPage) extends StateServiceResult

case class GetMetricHistory(checkRef: CheckRef, from: Option[DateTime], to: Option[DateTime], limit: Int, last: Option[DateTime]) extends StateServiceQuery
case class GetMetricHistoryResult(op: GetMetricHistory, page: CheckMetricsPage) extends StateServiceResult
