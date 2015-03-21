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
import scala.util.{Success, Try}
import java.util.UUID

import io.mandelbrot.core.model._

/**
 *
 */
class ScalarProcessor extends BehaviorProcessor {

  def enter(probe: ProbeInterface): Option[EventEffect] = {
    if (probe.lifecycle == ProbeInitializing) {
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val status = probe.getProbeStatus.copy(lifecycle = ProbeJoining, health = ProbeUnknown, lastUpdate = Some(timestamp), lastChange = Some(timestamp))
      Some(EventEffect(status, Vector.empty))
    } else None
  }

  def update(probe: ProbeInterface, processor: BehaviorProcessor): Option[EventEffect] = None

    /*
     * if we receive a status message while joining or known, then update probe state
     * and send notifications.  if the previous lifecycle was joining, then we move to
     * known.  if we transition from non-healthy to healthy, then we clear the correlation
     * and acknowledgement (if set).  if we transition from healthy to non-healthy, then
     * we set the correlation if it is different from the current correlation, and we start
     * the alert timer.
     */
  def processEvaluation(probe: ProbeInterface, command: ProcessProbeEvaluation): Try[CommandEffect] = {
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val summary = command.evaluation.summary.orElse(probe.summary)
      val health = command.evaluation.health.getOrElse(probe.health)
      val metrics = command.evaluation.metrics.getOrElse(Map.empty)
      val lastUpdate = Some(timestamp)
      var lifecycle = probe.lifecycle
      var lastChange = probe.lastChange
      var correlationId = probe.correlationId
      var acknowledgementId = probe.acknowledgementId

      // update lifecycle
      if (lifecycle == ProbeJoining)
        lifecycle = ProbeKnown
      if (health != probe.health) {
        lastChange = Some(timestamp)
      }
      // we are healthy
      if (health == ProbeHealthy) {
        correlationId = None
        acknowledgementId = None
      }
      // we are non-healthy
      else {
        if (probe.correlationId.isEmpty)
          correlationId = Some(UUID.randomUUID())
      }

      val status = ProbeStatus(timestamp, lifecycle, summary, health, metrics, lastUpdate, lastChange, correlationId, acknowledgementId, probe.squelch)

      var notifications = Vector.empty[ProbeNotification]
      // append lifecycle notification
      if (lifecycle != probe.lifecycle) {
        notifications = notifications :+ NotifyLifecycleChanges(probe.probeRef, command.evaluation.timestamp, probe.lifecycle, lifecycle)
      }
      // append health notification
      if (probe.health != health) {
        notifications = notifications :+ NotifyHealthChanges(probe.probeRef, command.evaluation.timestamp, correlationId, probe.health, health)
      } else {
        notifications = notifications :+ NotifyHealthUpdates(probe.probeRef, command.evaluation.timestamp, correlationId, health)
      }
      // append recovery notification
      if (health == ProbeHealthy && probe.acknowledgementId.isDefined) {
        notifications = notifications :+ NotifyRecovers(probe.probeRef, timestamp, probe.correlationId.get, probe.acknowledgementId.get)
      }

      Success(CommandEffect(ProcessProbeEvaluationResult(command), status, notifications))
  }

  /* ignore child messages */
  def processChild(probe: ProbeInterface, child: ProbeRef, status: ProbeStatus): Option[EventEffect] = None

  /*
   * if we haven't received a status message within the current expiry window, then update probe
   * state and send notifications.  probe health becomes unknown, and correlation is set if it is
   * different from the current correlation.  we restart the expiry timer, and we start the alert
   * timer if it is not already running.
   */
  def processExpiryTimeout(probe: ProbeInterface): Option[EventEffect] = {
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val health = ProbeUnknown
      val correlationId = if (probe.correlationId.isDefined) probe.correlationId else Some(UUID.randomUUID())
      var lastChange = probe.lastChange
      if (health != probe.health) {
        lastChange = Some(timestamp)
      }
      val status = probe.getProbeStatus(timestamp).copy(health = health, summary = None, lastChange = lastChange, correlation = correlationId)
      println("expiry status is " + status.toString)
      // append health notification
      val notifications = if (probe.health != health) {
        Vector(NotifyHealthChanges(probe.probeRef, timestamp, correlationId, probe.health, health))
      } else {
        Vector(NotifyHealthExpires(probe.probeRef, timestamp, correlationId))
      }
      // update state and send notifications
      Some(EventEffect(status, notifications))
  }

  /*
   * if the alert timer expires, then send a health-alerts notification and restart the alert timer.
   */
  def processAlertTimeout(probe: ProbeInterface): Option[EventEffect] = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val status = probe.getProbeStatus(timestamp)
    // send alert notification
    val notifications = probe.correlationId.map { correlation =>
      NotifyHealthAlerts(probe.probeRef, timestamp, probe.health, correlation, probe.acknowledgementId)
    }.toVector
    Some(EventEffect(status, notifications))
  }

}

class ScalarProbe extends ProbeBehaviorExtension {
  override def implement(properties: Map[String, String]): BehaviorProcessor = new ScalarProcessor
}