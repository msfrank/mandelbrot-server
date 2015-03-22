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

import java.util.UUID

import org.joda.time.{DateTimeZone, DateTime}
import scala.collection.mutable
import scala.util.{Try, Failure}

import io.mandelbrot.core.{BadRequest, ApiException}
import io.mandelbrot.core.model._

case class AggregateProbeSettings(evaluation: AggregateEvaluation)

/**
 *
 */
class AggregateProcessor(settings: AggregateProbeSettings) extends BehaviorProcessor {

  val evaluation = settings.evaluation
  var childrenStatus: Map[ProbeRef,Option[ProbeStatus]] = Map.empty

  def configure(status: ProbeStatus, children: Set[ProbeRef]): ConfigEffect = {
    childrenStatus = children.map(child => child -> None).toMap
    val initial = if (status.lifecycle == ProbeInitializing) {
      val timestamp = DateTime.now(DateTimeZone.UTC)
      status.copy(lifecycle = ProbeSynthetic, health = ProbeUnknown, lastUpdate = Some(timestamp), lastChange = Some(timestamp))
    } else status
    ConfigEffect(initial, Vector.empty, childrenStatus.keySet, Set.empty)
  }

  /* ignore probe evaluations from client */
  def processEvaluation(probe: ProbeInterface, command: ProcessProbeEvaluation): Try[CommandEffect] = Failure(ApiException(BadRequest))

  /* process the status of a child probe */
  def processChild(probe: ProbeInterface, childRef: ProbeRef, childStatus: ProbeStatus): Option[EventEffect] = {
    childrenStatus = childrenStatus + (childRef -> Some(childStatus))

    val timestamp = DateTime.now(DateTimeZone.UTC)
    val health = evaluation.evaluate(childrenStatus)
    val correlationId = if (health == ProbeHealthy) None else {
      if (probe.correlationId.isDefined) probe.correlationId else Some(UUID.randomUUID())
    }
    val lastUpdate = Some(timestamp)
    val lastChange = if (health == probe.health) probe.lastChange else {
      Some(timestamp)
    }

    var acknowledgementId: Option[UUID] = None

    // we are healthy
    if (health == ProbeHealthy) {
      acknowledgementId = None
    }
    // we are non-healthy
    else {
      acknowledgementId = probe.acknowledgementId
    }

    val status = ProbeStatus(timestamp, probe.lifecycle, None, health, Map.empty, lastUpdate, lastChange, correlationId, acknowledgementId, probe.squelch)
    var notifications = Vector.empty[ProbeNotification]

    // append health notification
    if (probe.health != health) {
      notifications = notifications :+ NotifyHealthChanges(probe.probeRef, timestamp, correlationId, probe.health, health)
    }
    // append recovery notification
    if (health == ProbeHealthy && probe.acknowledgementId.isDefined) {
      notifications = notifications :+ NotifyRecovers(probe.probeRef, timestamp, probe.correlationId.get, probe.acknowledgementId.get)
    }
    Some(EventEffect(status, notifications))
  }

  /* ignore spurious ProbeExpiryTimeout messages */
  def processExpiryTimeout(probe: ProbeInterface): Option[EventEffect] = None

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

class AggregateProbe extends ProbeBehaviorExtension {
  type Settings = AggregateProbeSettings
  class AggregateProcessorFactory(val settings: AggregateProbeSettings) extends DependentProcessorFactory {
    def implement() = new AggregateProcessor(settings)
  }
  def configure(properties: Map[String,String]) = {
    new AggregateProcessorFactory(AggregateProbeSettings(EvaluateWorst))
  }
}