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

  def configure(status: ProbeStatus, children: Set[ProbeRef]): ConfigEffect

  def processEvaluation(probe: ProbeInterface, command: ProcessProbeEvaluation): Try[CommandEffect]
  def processChild(probe: ProbeInterface, child: ProbeRef, status: ProbeStatus): Option[EventEffect]
  def processAlertTimeout(probe: ProbeInterface): Option[EventEffect]
  def processExpiryTimeout(probe: ProbeInterface): Option[EventEffect]

  /**
   *
   */
  def processEvent(probe: ProbeInterface, message: Any): Option[EventEffect] = message match {
    case ChildMutates(child, status) => processChild(probe, child, status)
    case ProbeAlertTimeout => processAlertTimeout(probe)
    case ProbeExpiryTimeout => processExpiryTimeout(probe)
    case _ => throw new IllegalArgumentException()
  }

  def processAcknowledge(probe: ProbeInterface, command: AcknowledgeProbe): Try[CommandEffect] = {
    probe.correlationId match {
      case None =>
        Failure(ApiException(ResourceNotFound))
      case Some(correlation) if probe.acknowledgementId.isDefined =>
        Failure(ApiException(Conflict))
      case Some(correlation) if correlation != command.correlationId =>
        Failure(ApiException(BadRequest))
      case Some(correlation) =>
        val acknowledgement = UUID.randomUUID()
        val timestamp = DateTime.now(DateTimeZone.UTC)
        val status = probe.getProbeStatus(timestamp).copy(acknowledged = Some(acknowledgement))
        val condition = ProbeCondition(timestamp, status.lifecycle, status.summary, status.health,
          status.correlation, status.acknowledged, status.squelched)
        val notifications = Vector(NotifyAcknowledged(probe.probeRef, timestamp, correlation, acknowledgement))
        Success(CommandEffect(AcknowledgeProbeResult(command, condition), status, notifications))
    }
  }

  def processUnacknowledge(probe: ProbeInterface, command: UnacknowledgeProbe): Try[CommandEffect] = {
    probe.acknowledgementId match {
      case None =>
        Failure(ApiException(ResourceNotFound))
      case Some(acknowledgement) if acknowledgement != command.acknowledgementId =>
        Failure(ApiException(BadRequest))
      case Some(acknowledgement) =>
        val timestamp = DateTime.now(DateTimeZone.UTC)
        val correlation = probe.correlationId.get
        val status = probe.getProbeStatus(timestamp).copy(acknowledged = None)
        val condition = ProbeCondition(timestamp, status.lifecycle, status.summary, status.health,
          status.correlation, status.acknowledged, status.squelched)
        val notifications = Vector(NotifyUnacknowledged(probe.probeRef, timestamp, correlation, acknowledgement))
        Success(CommandEffect(UnacknowledgeProbeResult(command, condition), status, notifications))
    }
  }

  def processSetSquelch(probe: ProbeInterface, command: SetProbeSquelch): Try[CommandEffect] = {
    if (probe.squelch == command.squelch) Failure(ApiException(BadRequest)) else {
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val squelch = command.squelch
      val status = probe.getProbeStatus(timestamp).copy(squelched = squelch)
      val condition = ProbeCondition(timestamp, status.lifecycle, status.summary, status.health,
        status.correlation, status.acknowledged, status.squelched)
      val notifications = if (command.squelch) Vector(NotifySquelched(probe.probeRef, timestamp)) else Vector(NotifyUnsquelched(probe.probeRef, timestamp))
      Success(CommandEffect(SetProbeSquelchResult(command, condition), status, notifications))
    }
  }

  def processCommand(probe: ProbeInterface, command: ProbeCommand): Try[CommandEffect] = command match {
    case cmd: ProcessProbeEvaluation => processEvaluation(probe, cmd)
    case cmd: AcknowledgeProbe => processAcknowledge(probe, cmd)
    case cmd: UnacknowledgeProbe => processUnacknowledge(probe, cmd)
    case cmd: SetProbeSquelch => processSetSquelch(probe, cmd)
    case _ => throw new IllegalArgumentException()
  }

  /*
   * probe lifecycle is leaving and the leaving timeout has expired.  probe lifecycle is set to
   * retired, state is updated, and lifecycle-changes notification is sent.  finally, all timers
   * are stopped, then the actor itself is stopped.
   */
  def retire(probe: ProbeInterface, lsn: Long): Option[EventEffect] = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val status = probe.getProbeStatus(timestamp).copy(lifecycle = ProbeRetired, lastChange = Some(timestamp), lastUpdate = Some(timestamp))
    val notifications = Vector(NotifyLifecycleChanges(probe.probeRef, timestamp, probe.lifecycle, ProbeRetired))
    Some(EventEffect(status, notifications))
  }
}

sealed trait ProbeEffect
case class CommandEffect(result: ProbeResult,
                         status: ProbeStatus,
                         notifications: Vector[ProbeNotification]) extends ProbeEffect

case class EventEffect(status: ProbeStatus,
                       notifications: Vector[ProbeNotification]) extends ProbeEffect

case class ConfigEffect(status: ProbeStatus,
                        notifications: Vector[ProbeNotification],
                        children: Set[ProbeRef],
                        metrics: Set[MetricSource]) extends ProbeEffect

