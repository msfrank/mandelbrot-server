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
import scala.concurrent.duration._
import java.util.UUID

import io.mandelbrot.core.notification._
import io.mandelbrot.core.metrics._

/**
 * Contains the metrics probe behavior policy.
 */
case class MetricsProbeBehavior(evaluation: MetricsEvaluation, flapWindow: FiniteDuration, flapDeviations: Int) extends ProbeBehavior {
  def makeProbeBehavior(): ProbeBehaviorInterface = new MetricsProbeBehaviorImpl(evaluation)
}

/**
 * Implements metrics probe behavior.
 */
class MetricsProbeBehaviorImpl(evaluation: MetricsEvaluation) extends ProbeBehaviorInterface {

  val metrics = new MetricsStore(evaluation)
  val flapQueue: Option[FlapQueue] = None

  def enter(probe: ProbeInterface): Option[EventMutation] = {
    probe.alertTimer.stop()
    probe.expiryTimer.restart(probe.policy.joiningTimeout)
    metrics.sources.map(_.probePath).toSet.foreach(probe.subscribeToMetrics)
    if (probe.lifecycle == ProbeInitializing) {
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val status = probe.getProbeStatus.copy(lifecycle = ProbeJoining, health = ProbeUnknown,
        summary = Some(evaluation.toString), lastUpdate = Some(timestamp), lastChange = Some(timestamp))
      Some(EventMutation(status, Vector.empty))
    } else None
  }

  /*
   * if the set of direct children has changed, or the probe policy has updated,
   * then update our state.
   */
  def update(probe: ProbeInterface, policy: ProbeBehavior): Option[EventMutation] = None

  /* ignore status messages */
  def processStatus(probe: ProbeInterface, message: StatusMessage): Option[EventMutation] = None

  /* ignore child messages */
  def processChild(probe: ProbeInterface, message: ProbeStatus): Option[EventMutation] = None

  /*
   * if we receive a metrics message while joining or known, then update probe state
   * and send notifications.  if the previous lifecycle was joining, then we move to
   * known.  if we transition from non-healthy to healthy, then we clear the correlation
   * and acknowledgement (if set).  if we transition from healthy to non-healthy, then
   * we set the correlation if it is different from the current correlation, and we start
   * the alert timer.
   */
  def processMetrics(probe: ProbeInterface, message: MetricsMessage): Option[EventMutation] = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val lastUpdate = Some(timestamp)

    var lifecycle = probe.lifecycle
    var health = probe.health
    var lastChange = probe.lastChange
    var correlationId = probe.correlationId
    var acknowledgementId = probe.acknowledgementId
    var notifications = Vector.empty[Notification]

    // push new metrics into the store
    message.metrics.foreach { case (metricName, metricValue) =>
      val source = MetricSource(message.source.path, metricName)
      metrics.append(source, metricValue)
    }
    // evaluate the store
    health = evaluation.evaluate(metrics) match {
      case Some(result) => if (result) ProbeFailed else ProbeHealthy
      case None => ProbeUnknown
    }
    correlationId = if (health == ProbeHealthy) None else {
      if (probe.correlationId.isDefined) probe.correlationId else Some(UUID.randomUUID())
    }
    // update lifecycle
    if (probe.lifecycle == ProbeJoining)
      lifecycle = ProbeKnown

    // reset the expiry timer
    probe.expiryTimer.reset(probe.policy.probeTimeout)

    // update last change
    if (health != probe.health) {
      flapQueue.foreach(_.push(message.timestamp))
      lastChange = Some(timestamp)
    }

    // we are healthy
    if (health == ProbeHealthy) {
      correlationId = None
      acknowledgementId = None
      probe.alertTimer.stop()
    }
    // we are non-healthy
    else {
      if (probe.correlationId != correlationId)
        probe.alertTimer.start(probe.policy.alertTimeout)
    }

    val status = ProbeStatus(probe.probeRef, timestamp, lifecycle, health, None, lastUpdate, lastChange, correlationId, acknowledgementId, probe.squelch)

    // append lifecycle notification
    if (probe.lifecycle != lifecycle)
      notifications = notifications :+ NotifyLifecycleChanges(probe.probeRef, message.timestamp, probe.lifecycle, lifecycle)
    // append health notification
    flapQueue match {
      case Some(flapDetector) if flapDetector.isFlapping =>
        notifications = notifications :+ NotifyHealthFlaps(probe.probeRef, message.timestamp, correlationId, flapDetector.flapStart)
      case _ if health != probe.health =>
        notifications = notifications :+ NotifyHealthChanges(probe.probeRef, message.timestamp, correlationId, probe.health, health)
      case _ =>
        notifications = notifications :+ NotifyHealthUpdates(probe.probeRef, message.timestamp, correlationId, health)
    }
    // append recovery notification
    if (probe.health == ProbeHealthy && probe.acknowledgementId.isDefined)
      notifications = notifications :+ NotifyRecovers(probe.probeRef, timestamp, probe.correlationId.get, probe.acknowledgementId.get)

    Some(EventMutation(status, notifications))
  }

  /*
   * if we haven't received a status message within the current expiry window, then update probe
   * state and send notifications.  probe health becomes unknown, and correlation is set if it is
   * different from the current correlation.  we restart the expiry timer, and we start the alert
   * timer if it is not already running.
   */
  def processExpiryTimeout(probe: ProbeInterface) = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val correlationId = if (probe.correlationId.isDefined) probe.correlationId else Some(UUID.randomUUID())
    // update health
    val health = ProbeUnknown
    val lastChange = if (probe.health == health) probe.lastChange else {
      flapQueue.foreach(_.push(timestamp))
      Some(timestamp)
    }
    // we transition from healthy to non-healthy
    if (!probe.alertTimer.isRunning)
      probe.alertTimer.start(probe.policy.alertTimeout)
    // reset the expiry timer
    probe.expiryTimer.reset(probe.policy.probeTimeout)
    val status = probe.getProbeStatus(timestamp).copy(health = health, lastChange = lastChange, correlation = correlationId)
    // append health notification
    val notifications = flapQueue match {
      case Some(flapDetector) if flapDetector.isFlapping =>
        Vector(NotifyHealthFlaps(probe.probeRef, timestamp, probe.correlationId, flapDetector.flapStart))
      case _ if health != probe.health =>
        Vector(NotifyHealthChanges(probe.probeRef, timestamp, probe.correlationId, probe.health, health))
      case _ =>
        Vector(NotifyHealthExpires(probe.probeRef, timestamp, probe.correlationId))
    }
    Some(EventMutation(status, notifications))
  }

  /*
   * if the alert timer expires, then send a health-alerts notification and restart the alert timer.
   */
  def processAlertTimeout(probe: ProbeInterface): Option[EventMutation] = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val status = probe.getProbeStatus(timestamp)
    // restart the alert timer
    probe.alertTimer.restart(probe.policy.alertTimeout)
    // send alert notification
    val notifications = probe.correlationId.map { correlation =>
      NotifyHealthAlerts(probe.probeRef, timestamp, probe.health, correlation, probe.acknowledgementId)
    }.toVector
    Some(EventMutation(status, notifications))
  }

  /*
   * probe lifecycle is leaving and the leaving timeout has expired.  probe lifecycle is set to
   * retired, state is updated, and lifecycle-changes notification is sent.  finally, all timers
   * are stopped, then the actor itself is stopped.
   */
  def retire(probe: ProbeInterface, lsn: Long): Option[EventMutation] = {
    probe.expiryTimer.stop()
    probe.alertTimer.stop()
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val status = probe.getProbeStatus(timestamp).copy(lifecycle = ProbeRetired, lastChange = Some(timestamp), lastUpdate = Some(timestamp))
    val notifications = Vector(NotifyLifecycleChanges(probe.probeRef, timestamp, probe.lifecycle, ProbeRetired))
    Some(EventMutation(status, notifications))
  }

  def exit(probe: ProbeInterface): Option[EventMutation] = {
    // stop timers
    probe.alertTimer.stop()
    metrics.sources.map(_.probePath).toSet.foreach(probe.unsubscribeFromMetrics)
    None
  }
}

