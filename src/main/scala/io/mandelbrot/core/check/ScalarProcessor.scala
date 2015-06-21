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

package io.mandelbrot.core.check

import org.joda.time.{DateTimeZone, DateTime}
import scala.util.{Success, Try}
import java.util.UUID

import io.mandelbrot.core.model._

/**
 *
 */
class ScalarProcessor extends BehaviorProcessor {

  def initialize(): InitializeEffect = InitializeEffect(None)

  def configure(status: CheckStatus, children: Set[CheckRef]): ConfigureEffect = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val initial = if (status.lifecycle == CheckInitializing) {
      status.copy(lifecycle = CheckJoining, health = CheckUnknown, lastUpdate = Some(timestamp), lastChange = Some(timestamp))
    } else status
    ConfigureEffect(initial, Vector.empty, Set.empty, Set.empty)
  }

  /*
   * if we receive a status message while joining or known, then update check state
   * and send notifications.  if the previous lifecycle was joining, then we move to
   * known.  if we transition from non-healthy to healthy, then we clear the correlation
   * and acknowledgement (if set).  if we transition from healthy to non-healthy, then
   * we set the correlation if it is different from the current correlation, and we start
   * the alert timer.
   */
  def processEvaluation(check: AccessorOps, command: ProcessCheckEvaluation): Try[CommandEffect] = {
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val summary = command.evaluation.summary.orElse(check.summary)
      val health = command.evaluation.health.getOrElse(check.health)
      val metrics = command.evaluation.metrics.getOrElse(Map.empty)
      val lastUpdate = Some(timestamp)
      var lifecycle = check.lifecycle
      var lastChange = check.lastChange
      var correlationId = check.correlationId
      var acknowledgementId = check.acknowledgementId

      // update lifecycle
      if (lifecycle == CheckJoining)
        lifecycle = CheckKnown
      if (health != check.health) {
        lastChange = Some(timestamp)
      }
      // we are healthy
      if (health == CheckHealthy) {
        correlationId = None
        acknowledgementId = None
      }
      // we are non-healthy
      else {
        if (check.correlationId.isEmpty)
          correlationId = Some(UUID.randomUUID())
      }

      val status = CheckStatus(timestamp, lifecycle, summary, health, metrics, lastUpdate, lastChange, correlationId, acknowledgementId, check.squelch)

      var notifications = Vector.empty[CheckNotification]
      // append lifecycle notification
      if (lifecycle != check.lifecycle) {
        notifications = notifications :+ NotifyLifecycleChanges(check.checkRef, command.evaluation.timestamp, check.lifecycle, lifecycle)
      }
      // append health notification
      if (check.health != health) {
        notifications = notifications :+ NotifyHealthChanges(check.checkRef, command.evaluation.timestamp, correlationId, check.health, health)
      } else {
        notifications = notifications :+ NotifyHealthUpdates(check.checkRef, command.evaluation.timestamp, correlationId, health)
      }
      // append recovery notification
      if (health == CheckHealthy && check.acknowledgementId.isDefined) {
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
  def processExpiryTimeout(check: AccessorOps): Option[EventEffect] = {
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val health = CheckUnknown
      val correlationId = if (check.correlationId.isDefined) check.correlationId else Some(UUID.randomUUID())
      var lastChange = check.lastChange
      if (health != check.health) {
        lastChange = Some(timestamp)
      }
      val status = check.getCheckStatus(timestamp).copy(health = health, summary = None, lastChange = lastChange, correlation = correlationId)
      println("expiry status is " + status.toString)
      // append health notification
      val notifications = if (check.health != health) {
        Vector(NotifyHealthChanges(check.checkRef, timestamp, correlationId, check.health, health))
      } else {
        Vector(NotifyHealthExpires(check.checkRef, timestamp, correlationId))
      }
      // update state and send notifications
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

case class ScalarCheckSettings()

class ScalarCheck extends CheckBehaviorExtension {
  type Settings = ScalarCheckSettings
  class ScalarProcessorFactory(val settings: ScalarCheckSettings) extends DependentProcessorFactory {
    def implement() = new ScalarProcessor
  }
  def configure(properties: Map[String,String]) = new ScalarProcessorFactory(ScalarCheckSettings())
}
