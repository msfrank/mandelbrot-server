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

case class MetricsCheckSettings(evaluation: MetricsEvaluation)

/**
 * Implements metrics check behavior.
 */
class MetricsProcessor(settings: MetricsCheckSettings) extends BehaviorProcessor {

  val evaluation = settings.evaluation
  val metricsStore = new MetricsStore(settings.evaluation)

  def initialize(): InitializeEffect = InitializeEffect(None)

  def configure(status: CheckStatus, children: Set[CheckRef]): ConfigureEffect = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val initial = if (status.lifecycle == CheckInitializing) {
      status.copy(lifecycle = CheckJoining, health = CheckUnknown, summary = Some(evaluation.toString),
        lastUpdate = Some(timestamp), lastChange = Some(timestamp))
    } else status
    ConfigureEffect(initial, Vector.empty, Set.empty, evaluation.sources)
  }

  /*
   * if we receive a metrics message while joining or known, then update check state
   * and send notifications.  if the previous lifecycle was joining, then we move to
   * known.  if we transition from non-healthy to healthy, then we clear the correlation
   * and acknowledgement (if set).  if we transition from healthy to non-healthy, then
   * we set the correlation if it is different from the current correlation, and we start
   * the alert timer.
   */
  def processEvaluation(check: AccessorOps, command: ProcessCheckEvaluation): Try[CommandEffect] = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val lastUpdate = Some(timestamp)
    val metrics = command.evaluation.metrics.getOrElse(Map.empty)

    var lifecycle = check.lifecycle
    var health = check.health
    var lastChange = check.lastChange
    var correlationId = check.correlationId
    var acknowledgementId = check.acknowledgementId

    // push new metrics into the store
    metrics.foreach { case (metricName, metricValue) =>
      val source = MetricSource(check.checkRef.checkId, metricName)
      metricsStore.append(source, metricValue)
    }

    // evaluate the store
    health = evaluation.evaluate(metricsStore) match {
      case Some(result) => if (result) CheckFailed else CheckHealthy
      case None => CheckUnknown
    }
    correlationId = if (health == CheckHealthy) None else {
      if (check.correlationId.isDefined) check.correlationId else Some(UUID.randomUUID())
    }
    // update lifecycle
    if (check.lifecycle == CheckJoining) {
      lifecycle = CheckKnown
    }

    // update last change
    if (health != check.health) {
      lastChange = Some(timestamp)
    }

    // we are healthy
    if (health == CheckHealthy) {
      correlationId = None
      acknowledgementId = None
    }

    val status = CheckStatus(timestamp, lifecycle, None, health, metrics, lastUpdate, lastChange, correlationId, acknowledgementId, check.squelch)
    var notifications = Vector.empty[CheckNotification]

    // append lifecycle notification
    if (check.lifecycle != lifecycle)
      notifications = notifications :+ NotifyLifecycleChanges(check.checkRef, command.evaluation.timestamp, check.lifecycle, lifecycle)
    // append health notification
    if (health != check.health) {
      notifications = notifications :+ NotifyHealthChanges(check.checkRef, command.evaluation.timestamp, correlationId, check.health, health)
    } else {
      notifications = notifications :+ NotifyHealthUpdates(check.checkRef, command.evaluation.timestamp, correlationId, health)
    }
    // append recovery notification
    if (check.health == CheckHealthy && check.acknowledgementId.isDefined) {
      notifications = notifications :+ NotifyRecovers(check.checkRef, timestamp, check.correlationId.get, check.acknowledgementId.get)
    }

    Success(CommandEffect(ProcessCheckEvaluationResult(command), status, notifications))
  }

  /* ignore child messages */
  def processChild(check: AccessorOps, child: CheckRef, status: CheckStatus): Option[EventEffect] = None

  /*
   * if we haven't received a status message within the current expiry window, then update check
   * state and send notifications.  check health becomes unknown, and correlation is set if it is
   * different from the current correlation.  we restart the expiry timer, and we start the alert
   * timer if it is not already running.
   */
  def processExpiryTimeout(check: AccessorOps) = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val correlationId = if (check.correlationId.isDefined) check.correlationId else Some(UUID.randomUUID())
    // update health
    val health = CheckUnknown
    val lastChange = if (check.health == health) check.lastChange else {
      Some(timestamp)
    }
    val status = check.getCheckStatus(timestamp).copy(health = health, lastChange = lastChange, correlation = correlationId)
    // append health notification
    val notifications = if (health != check.health) {
      Vector(NotifyHealthChanges(check.checkRef, timestamp, check.correlationId, check.health, health))
    } else {
      Vector(NotifyHealthExpires(check.checkRef, timestamp, check.correlationId))
    }
    Some(EventEffect(status, notifications))
  }

  /*
   * if the alert timer expires, then send a health-alerts notification and restart the alert timer.
   */
  def processAlertTimeout(check: AccessorOps): Option[EventEffect] = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val status = check.getCheckStatus(timestamp)
    // send alert notification
    val notifications = check.correlationId.map { correlation =>
      NotifyHealthAlerts(check.checkRef, timestamp, check.health, correlation, check.acknowledgementId)
    }.toVector
    Some(EventEffect(status, notifications))
  }
}

class MetricsCheck extends CheckBehaviorExtension {
  type Settings = MetricsCheckSettings
  class MetricsProcessorFactory(val settings: MetricsCheckSettings) extends DependentProcessorFactory {
    def implement() = new MetricsProcessor(settings)
  }
  def configure(properties: Map[String,String]) = {
    if (!properties.contains("evaluation"))
      throw new IllegalArgumentException("missing evaluation")
    val evaluation = MetricsEvaluationParser.parseMetricsEvaluation(properties("evaluation"))
    new MetricsProcessorFactory(MetricsCheckSettings(evaluation))
  }
}
