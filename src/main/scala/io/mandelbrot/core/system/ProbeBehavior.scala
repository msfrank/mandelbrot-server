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

import io.mandelbrot.core.{BadRequest, Conflict, ResourceNotFound, ApiException}
import org.joda.time.{DateTimeZone, DateTime}
import java.util.UUID

import io.mandelbrot.core.notification._

import scala.util.{Success, Failure, Try}

/**
 *
 */
trait ProbeBehavior {
  def makeProbeBehavior(): ProbeBehaviorInterface
}

/**
 *
 */
trait ProbeBehaviorInterface {
  import io.mandelbrot.core.system.Probe._

  def enter(probe: ProbeInterface): Option[EventMutation]
  def update(probe: ProbeInterface, policy: ProbeBehavior): Option[EventMutation]
  def processStatus(probe: ProbeInterface, message: StatusMessage): Option[EventMutation]
  def processMetrics(probe: ProbeInterface, message: MetricsMessage): Option[EventMutation]
  def processChild(probe: ProbeInterface, message: ProbeStatus): Option[EventMutation]
  def processAlertTimeout(probe: ProbeInterface): Option[EventMutation]
  def processExpiryTimeout(probe: ProbeInterface): Option[EventMutation]
  def retire(probe: ProbeInterface, lsn: Long): Option[EventMutation]
  def exit(probe: ProbeInterface): Option[EventMutation]

  /**
   *
   */
  def processEvent(probe: ProbeInterface, message: Any): Option[EventMutation] = message match {
    case ProbeEnters => enter(probe)
    case msg: StatusMessage => processStatus(probe, msg)
    case msg: MetricsMessage => processMetrics(probe, msg)
    case msg: ProbeStatus => processChild(probe, msg)
    case ProbeAlertTimeout => processAlertTimeout(probe)
    case ProbeExpiryTimeout => processExpiryTimeout(probe)
    case ProbeExits => exit(probe)
    case _ => throw new IllegalArgumentException()
  }

  def processAcknowledge(probe: ProbeInterface, command: AcknowledgeProbe): Try[CommandMutation] = {
    probe.correlationId match {
      case None =>
        Failure(new ApiException(ResourceNotFound))
      case Some(correlation) if probe.acknowledgementId.isDefined =>
        Failure(new ApiException(Conflict))
      case Some(correlation) if correlation != command.correlationId =>
        Failure(new ApiException(BadRequest))
      case Some(correlation) =>
        val acknowledgement = UUID.randomUUID()
        val timestamp = DateTime.now(DateTimeZone.UTC)
        val status = probe.getProbeStatus(timestamp).copy(acknowledged = Some(acknowledgement))
        val notifications = Vector(NotifyAcknowledged(probe.probeRef, timestamp, correlation, acknowledgement))
        Success(CommandMutation(AcknowledgeProbeResult(command, acknowledgement), status, notifications))
    }
  }

  def processUnacknowledge(probe: ProbeInterface, command: UnacknowledgeProbe): Try[CommandMutation] = {
    probe.acknowledgementId match {
      case None =>
        Failure(new ApiException(ResourceNotFound))
      case Some(acknowledgement) if acknowledgement != command.acknowledgementId =>
        Failure(new ApiException(BadRequest))
      case Some(acknowledgement) =>
        val timestamp = DateTime.now(DateTimeZone.UTC)
        val correlation = probe.correlationId.get
        val status = probe.getProbeStatus(timestamp).copy(acknowledged = None)
        val notifications = Vector(NotifyUnacknowledged(probe.probeRef, timestamp, correlation, acknowledgement))
        Success(CommandMutation(UnacknowledgeProbeResult(command, acknowledgement), status, notifications))
    }
  }

  def processSetSquelch(probe: ProbeInterface, command: SetProbeSquelch): Try[CommandMutation] = {
    if (probe.squelch == command.squelch) Failure(new ApiException(BadRequest)) else {
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val squelch = command.squelch
      val status = probe.getProbeStatus(timestamp).copy(squelched = squelch)
      val notifications = if (command.squelch) Vector(NotifySquelched(probe.probeRef, timestamp)) else Vector(NotifyUnsquelched(probe.probeRef, timestamp))
      Success(CommandMutation(SetProbeSquelchResult(command, command.squelch), status, notifications))
    }
  }

  def processCommand(probe: ProbeInterface, command: ProbeCommand): Try[CommandMutation] = command match {
    case cmd: AcknowledgeProbe => processAcknowledge(probe, cmd)
    case cmd: UnacknowledgeProbe => processUnacknowledge(probe, cmd)
    case cmd: SetProbeSquelch => processSetSquelch(probe, cmd)
    case _ => throw new IllegalArgumentException()
  }
}
