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
import io.mandelbrot.core.metrics._

case class MetricsProbeSettings(evaluation: MetricsEvaluation)

/**
 * Implements metrics probe behavior.
 */
class MetricsProcessor(settings: MetricsProbeSettings) extends BehaviorProcessor {

  val evaluation = settings.evaluation
  val metricsStore = new MetricsStore(settings.evaluation)

  def initialize(): InitializeEffect = InitializeEffect(None)

  def configure(status: ProbeStatus, children: Set[ProbeRef]): ConfigureEffect = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val initial = if (status.lifecycle == ProbeInitializing) {
      status.copy(lifecycle = ProbeJoining, health = ProbeUnknown, summary = Some(evaluation.toString),
        lastUpdate = Some(timestamp), lastChange = Some(timestamp))
    } else status
    ConfigureEffect(initial, Vector.empty, Set.empty, evaluation.sources)
  }

  /*
   * if we receive a metrics message while joining or known, then update probe state
   * and send notifications.  if the previous lifecycle was joining, then we move to
   * known.  if we transition from non-healthy to healthy, then we clear the correlation
   * and acknowledgement (if set).  if we transition from healthy to non-healthy, then
   * we set the correlation if it is different from the current correlation, and we start
   * the alert timer.
   */
  def processEvaluation(probe: ProbeInterface, command: ProcessProbeEvaluation): Try[CommandEffect] = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val lastUpdate = Some(timestamp)
    val metrics = command.evaluation.metrics.getOrElse(Map.empty)

    var lifecycle = probe.lifecycle
    var health = probe.health
    var lastChange = probe.lastChange
    var correlationId = probe.correlationId
    var acknowledgementId = probe.acknowledgementId

    // push new metrics into the store
    metrics.foreach { case (metricName, metricValue) =>
      val source = MetricSource(probe.probeRef.path, metricName)
      metricsStore.append(source, metricValue)
    }

    // evaluate the store
    health = evaluation.evaluate(metricsStore) match {
      case Some(result) => if (result) ProbeFailed else ProbeHealthy
      case None => ProbeUnknown
    }
    correlationId = if (health == ProbeHealthy) None else {
      if (probe.correlationId.isDefined) probe.correlationId else Some(UUID.randomUUID())
    }
    // update lifecycle
    if (probe.lifecycle == ProbeJoining) {
      lifecycle = ProbeKnown
    }

    // update last change
    if (health != probe.health) {
      lastChange = Some(timestamp)
    }

    // we are healthy
    if (health == ProbeHealthy) {
      correlationId = None
      acknowledgementId = None
    }

    val status = ProbeStatus(timestamp, lifecycle, None, health, metrics, lastUpdate, lastChange, correlationId, acknowledgementId, probe.squelch)
    var notifications = Vector.empty[ProbeNotification]

    // append lifecycle notification
    if (probe.lifecycle != lifecycle)
      notifications = notifications :+ NotifyLifecycleChanges(probe.probeRef, command.evaluation.timestamp, probe.lifecycle, lifecycle)
    // append health notification
    if (health != probe.health) {
      notifications = notifications :+ NotifyHealthChanges(probe.probeRef, command.evaluation.timestamp, correlationId, probe.health, health)
    } else {
      notifications = notifications :+ NotifyHealthUpdates(probe.probeRef, command.evaluation.timestamp, correlationId, health)
    }
    // append recovery notification
    if (probe.health == ProbeHealthy && probe.acknowledgementId.isDefined) {
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
  def processExpiryTimeout(probe: ProbeInterface) = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val correlationId = if (probe.correlationId.isDefined) probe.correlationId else Some(UUID.randomUUID())
    // update health
    val health = ProbeUnknown
    val lastChange = if (probe.health == health) probe.lastChange else {
      Some(timestamp)
    }
    val status = probe.getProbeStatus(timestamp).copy(health = health, lastChange = lastChange, correlation = correlationId)
    // append health notification
    val notifications = if (health != probe.health) {
      Vector(NotifyHealthChanges(probe.probeRef, timestamp, probe.correlationId, probe.health, health))
    } else {
      Vector(NotifyHealthExpires(probe.probeRef, timestamp, probe.correlationId))
    }
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

class MetricsProbe extends ProbeBehaviorExtension {
  type Settings = MetricsProbeSettings
  class MetricsProcessorFactory(val settings: MetricsProbeSettings) extends DependentProcessorFactory {
    def implement() = new MetricsProcessor(settings)
  }
  def configure(properties: Map[String,String]) = {
    if (!properties.contains("evaluation"))
      throw new IllegalArgumentException("missing evaluation")
    val evaluation = MetricsEvaluationParser.parseMetricsEvaluation(properties("evaluation"))
    new MetricsProcessorFactory(MetricsProbeSettings(evaluation))
  }
}