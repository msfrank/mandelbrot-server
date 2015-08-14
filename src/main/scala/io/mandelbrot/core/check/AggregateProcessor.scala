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

import java.util.UUID

import org.joda.time.{DateTimeZone, DateTime}
import scala.collection.mutable
import scala.util.{Try, Failure}

import io.mandelbrot.core.{BadRequest, ApiException}
import io.mandelbrot.core.model._

case class AggregateCheckSettings(evaluation: AggregateEvaluation)

/**
 *
 */
class AggregateProcessor(settings: AggregateCheckSettings) extends BehaviorProcessor {

  val evaluation = settings.evaluation
  var childrenStatus: Map[CheckRef,Option[CheckStatus]] = Map.empty

  def initialize(check: AccessorOps): InitializeEffect = InitializeEffect(Map.empty)

  def configure(check: AccessorOps, results: Map[CheckId,Vector[CheckStatus]], children: Set[CheckRef]): ConfigureEffect = {
    childrenStatus = children.map(child => child -> None).toMap
    val status = check.getCheckStatus
    val initial = if (status.lifecycle == CheckInitializing) {
      val timestamp = DateTime.now(DateTimeZone.UTC)
      status.copy(lifecycle = CheckSynthetic, health = CheckUnknown, lastUpdate = Some(timestamp), lastChange = Some(timestamp))
    } else status
    ConfigureEffect(initial, Vector.empty, Set.empty)
  }

  /* ignore check evaluations from client */
  def processEvaluation(check: AccessorOps, command: ProcessCheckEvaluation): Try[CommandEffect] = Failure(ApiException(BadRequest))

  /* process the status of a child check */
  def processChild(check: AccessorOps, childRef: CheckRef, childStatus: CheckStatus): Option[EventEffect] = {
    childrenStatus = childrenStatus + (childRef -> Some(childStatus))

    val timestamp = DateTime.now(DateTimeZone.UTC)
    val health = evaluation.evaluate(childrenStatus)
    val correlationId = if (health == CheckHealthy) None else {
      if (check.correlationId.isDefined) check.correlationId else Some(UUID.randomUUID())
    }
    val lastUpdate = Some(timestamp)
    val lastChange = if (health == check.health) check.lastChange else {
      Some(timestamp)
    }

    var acknowledgementId: Option[UUID] = None

    // we are healthy
    if (health == CheckHealthy) {
      acknowledgementId = None
    }
    // we are non-healthy
    else {
      acknowledgementId = check.acknowledgementId
    }

    val status = CheckStatus(check.generation, timestamp, check.lifecycle, None, health, Map.empty,
      lastUpdate, lastChange, correlationId, acknowledgementId, check.squelch)
    var notifications = Vector.empty[CheckNotification]

    // append health notification
    if (check.health != health) {
      notifications = notifications :+ NotifyHealthChanges(check.checkRef, timestamp, correlationId, check.health, health)
    }
    // append recovery notification
    if (health == CheckHealthy && check.acknowledgementId.isDefined) {
      notifications = notifications :+ NotifyRecovers(check.checkRef, timestamp, check.correlationId.get, check.acknowledgementId.get)
    }
    Some(EventEffect(status, notifications))
  }

  /* ignore spurious CheckExpiryTimeout$ messages */
  def processExpiryTimeout(check: AccessorOps): Option[EventEffect] = None

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

class AggregateCheck extends CheckBehaviorExtension {
  type Settings = AggregateCheckSettings
  class AggregateProcessorFactory(val settings: AggregateCheckSettings) extends DependentProcessorFactory {
    def implement() = new AggregateProcessor(settings)
  }
  def configure(properties: Map[String,String]) = {
    new AggregateProcessorFactory(AggregateCheckSettings(EvaluateWorst))
  }
}
