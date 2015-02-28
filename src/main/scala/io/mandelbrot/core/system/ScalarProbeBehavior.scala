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
import scala.concurrent.duration.FiniteDuration
import scala.util.{Success, Try}
import java.util.UUID

import io.mandelbrot.core.notification._

/**
 *
 */
case class ScalarProbeBehavior(flapWindow: FiniteDuration, flapDeviations: Int) extends ProbeBehavior {
  def makeProbeBehavior() = new ScalarProbeBehaviorImpl()
}

/**
 *
 */
class ScalarProbeBehaviorImpl extends ProbeBehaviorInterface {

  val flapQueue: Option[FlapQueue] = None

  def enter(probe: ProbeInterface): Option[EventEffect] = {
    probe.alertTimer.stop()
    probe.expiryTimer.restart(probe.policy.joiningTimeout)
    if (probe.lifecycle == ProbeInitializing) {
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val status = probe.getProbeStatus.copy(lifecycle = ProbeJoining, health = ProbeUnknown, lastUpdate = Some(timestamp), lastChange = Some(timestamp))
      Some(EventEffect(status, Vector.empty))
    } else None
  }

  def update(probe: ProbeInterface, policy: ProbeBehavior): Option[EventEffect] = None

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
      // reset the expiry timer
      probe.expiryTimer.reset(probe.policy.probeTimeout)
      if (health != probe.health) {
        lastChange = Some(timestamp)
        flapQueue.foreach(_.push(command.evaluation.timestamp))
      }
      // we are healthy
      if (health == ProbeHealthy) {
        correlationId = None
        acknowledgementId = None
        probe.alertTimer.stop()
      }
      // we are non-healthy
      else {
        if (probe.correlationId.isEmpty)
          correlationId = Some(UUID.randomUUID())
        if (!probe.alertTimer.isRunning)
          probe.alertTimer.start(probe.policy.alertTimeout)
      }

      val status = ProbeStatus(timestamp, lifecycle, summary, health, metrics, lastUpdate, lastChange, correlationId, acknowledgementId, probe.squelch)

      var notifications = Vector.empty[NotificationEvent]
      // append lifecycle notification
      if (lifecycle != probe.lifecycle)
        notifications = notifications :+ NotifyLifecycleChanges(probe.probeRef, command.evaluation.timestamp, probe.lifecycle, lifecycle)
      // append health notification
      flapQueue match {
        case Some(flapDetector) if flapDetector.isFlapping =>
          notifications = notifications :+ NotifyHealthFlaps(probe.probeRef, command.evaluation.timestamp, correlationId, flapDetector.flapStart)
        case _ if probe.health != health =>
          notifications = notifications :+ NotifyHealthChanges(probe.probeRef, command.evaluation.timestamp, correlationId, probe.health, health)
        case _ =>
          notifications = notifications :+ NotifyHealthUpdates(probe.probeRef, command.evaluation.timestamp, correlationId, health)
      }
      // append recovery notification
      if (health == ProbeHealthy && probe.acknowledgementId.isDefined)
        notifications = notifications :+ NotifyRecovers(probe.probeRef, timestamp, probe.correlationId.get, probe.acknowledgementId.get)

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
        flapQueue.foreach(_.push(timestamp))
      }
      if (!probe.alertTimer.isRunning)
        probe.alertTimer.start(probe.policy.alertTimeout)
      // reset the expiry timer
      probe.expiryTimer.restart(probe.policy.probeTimeout)
      val status = probe.getProbeStatus(timestamp).copy(health = health, summary = None, lastChange = lastChange, correlation = correlationId)
      println("expiry status is " + status.toString)
      // append health notification
      val notifications = flapQueue match {
        case Some(flapDetector) if flapDetector.isFlapping =>
          Vector(NotifyHealthFlaps(probe.probeRef, timestamp, correlationId, flapDetector.flapStart))
        case _ if probe.health != health =>
          Vector(NotifyHealthChanges(probe.probeRef, timestamp, correlationId, probe.health, health))
        case _ =>
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
    // restart the alert timer
    probe.alertTimer.restart(probe.policy.alertTimeout)
    // send alert notification
    val notifications = probe.correlationId.map { correlation =>
      NotifyHealthAlerts(probe.probeRef, timestamp, probe.health, correlation, probe.acknowledgementId)
    }.toVector
    Some(EventEffect(status, notifications))
  }

  /*
   * probe lifecycle is leaving and the leaving timeout has expired.  probe lifecycle is set to
   * retired, state is updated, and lifecycle-changes notification is sent.  finally, all timers
   * are stopped, then the actor itself is stopped.
   */
  def retire(probe: ProbeInterface, lsn: Long): Option[EventEffect] = {
    probe.expiryTimer.stop()
    probe.alertTimer.stop()
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val status = probe.getProbeStatus(timestamp).copy(lifecycle = ProbeRetired, lastChange = Some(timestamp), lastUpdate = Some(timestamp))
    val notifications = Vector(NotifyLifecycleChanges(probe.probeRef, timestamp, probe.lifecycle, ProbeRetired))
    Some(EventEffect(status, notifications))  }

  def exit(probe: ProbeInterface): Option[EventEffect] = {
    probe.alertTimer.stop()
    probe.expiryTimer.stop()
    None
  }
}
