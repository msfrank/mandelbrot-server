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

package io.mandelbrot.core.system

import org.joda.time.{DateTimeZone, DateTime}
import scala.util.{Success, Failure, Try}
import java.util.UUID

import io.mandelbrot.core.model._
import io.mandelbrot.core.{BadRequest, Conflict, ResourceNotFound, ApiException}

/**
 *
 */
trait BehaviorProcessor {

  def initialize(): InitializeEffect

  def configure(status: CheckStatus, children: Set[CheckRef]): ConfigureEffect

  def processEvaluation(check: AccessorOps, command: ProcessCheckEvaluation): Try[CommandEffect]
  def processChild(check: AccessorOps, child: CheckRef, status: CheckStatus): Option[EventEffect]
  def processAlertTimeout(check: AccessorOps): Option[EventEffect]
  def processExpiryTimeout(check: AccessorOps): Option[EventEffect]

  /**
   *
   */
  def processEvent(check: AccessorOps, message: Any): Option[EventEffect] = message match {
    case ChildMutates(child, status) => processChild(check, child, status)
    case CheckAlertTimeout => processAlertTimeout(check)
    case CheckExpiryTimeout => processExpiryTimeout(check)
    case _ => throw new IllegalArgumentException()
  }

  def processAcknowledge(check: AccessorOps, command: AcknowledgeCheck): Try[CommandEffect] = {
    check.correlationId match {
      case None =>
        Failure(ApiException(ResourceNotFound))
      case Some(correlation) if check.acknowledgementId.isDefined =>
        Failure(ApiException(Conflict))
      case Some(correlation) if correlation != command.correlationId =>
        Failure(ApiException(BadRequest))
      case Some(correlation) =>
        val acknowledgement = UUID.randomUUID()
        val timestamp = DateTime.now(DateTimeZone.UTC)
        val status = check.getCheckStatus(timestamp).copy(acknowledged = Some(acknowledgement))
        val condition = CheckCondition(timestamp, status.lifecycle, status.summary, status.health,
          status.correlation, status.acknowledged, status.squelched)
        val notifications = Vector(NotifyAcknowledged(check.checkRef, timestamp, correlation, acknowledgement))
        Success(CommandEffect(AcknowledgeCheckResult(command, condition), status, notifications))
    }
  }

  def processUnacknowledge(check: AccessorOps, command: UnacknowledgeCheck): Try[CommandEffect] = {
    check.acknowledgementId match {
      case None =>
        Failure(ApiException(ResourceNotFound))
      case Some(acknowledgement) if acknowledgement != command.acknowledgementId =>
        Failure(ApiException(BadRequest))
      case Some(acknowledgement) =>
        val timestamp = DateTime.now(DateTimeZone.UTC)
        val correlation = check.correlationId.get
        val status = check.getCheckStatus(timestamp).copy(acknowledged = None)
        val condition = CheckCondition(timestamp, status.lifecycle, status.summary, status.health,
          status.correlation, status.acknowledged, status.squelched)
        val notifications = Vector(NotifyUnacknowledged(check.checkRef, timestamp, correlation, acknowledgement))
        Success(CommandEffect(UnacknowledgeCheckResult(command, condition), status, notifications))
    }
  }

  def processSetSquelch(check: AccessorOps, command: SetCheckSquelch): Try[CommandEffect] = {
    if (check.squelch == command.squelch) Failure(ApiException(BadRequest)) else {
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val squelch = command.squelch
      val status = check.getCheckStatus(timestamp).copy(squelched = squelch)
      val condition = CheckCondition(timestamp, status.lifecycle, status.summary, status.health,
        status.correlation, status.acknowledged, status.squelched)
      val notifications = if (command.squelch) Vector(NotifySquelched(check.checkRef, timestamp)) else Vector(NotifyUnsquelched(check.checkRef, timestamp))
      Success(CommandEffect(SetCheckSquelchResult(command, condition), status, notifications))
    }
  }

  def processCommand(check: AccessorOps, command: CheckCommand): Try[CommandEffect] = command match {
    case cmd: ProcessCheckEvaluation => processEvaluation(check, cmd)
    case cmd: AcknowledgeCheck => processAcknowledge(check, cmd)
    case cmd: UnacknowledgeCheck => processUnacknowledge(check, cmd)
    case cmd: SetCheckSquelch => processSetSquelch(check, cmd)
    case _ => throw new IllegalArgumentException()
  }

  /*
   * check lifecycle is leaving and the leaving timeout has expired.  check lifecycle is set to
   * retired, state is updated, and lifecycle-changes notification is sent.  finally, all timers
   * are stopped, then the actor itself is stopped.
   */
  def retire(check: AccessorOps, lsn: Long): Option[EventEffect] = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val status = check.getCheckStatus(timestamp).copy(lifecycle = CheckRetired, lastChange = Some(timestamp), lastUpdate = Some(timestamp))
    val notifications = Vector(NotifyLifecycleChanges(check.checkRef, timestamp, check.lifecycle, CheckRetired))
    Some(EventEffect(status, notifications))
  }
}

sealed trait CheckEffect
case class InitializeEffect(from: Option[DateTime]) extends CheckEffect
case class ConfigureEffect(status: CheckStatus,
                           notifications: Vector[CheckNotification],
                           children: Set[CheckRef],
                           metrics: Set[MetricSource]) extends CheckEffect
case class CommandEffect(result: CheckResult,
                         status: CheckStatus,
                         notifications: Vector[CheckNotification]) extends CheckEffect

case class EventEffect(status: CheckStatus,
                       notifications: Vector[CheckNotification]) extends CheckEffect

