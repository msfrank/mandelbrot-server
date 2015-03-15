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

/**
 *
 */
class AggregateProcessor(evaluation: AggregateEvaluation) extends BehaviorProcessor {

  val children = new mutable.HashMap[ProbeRef,Option[ProbeStatus]]
  val flapQueue: Option[FlapQueue] = None

  def enter(probe: ProbeInterface): Option[EventEffect] = {
    probe.children.foreach(child => children.put(child, None))
    probe.alertTimer.stop()
    probe.expiryTimer.restart(probe.policy.joiningTimeout)
    val status = if (probe.lifecycle == ProbeInitializing) {
      val timestamp = DateTime.now(DateTimeZone.UTC)
      probe.getProbeStatus.copy(lifecycle = ProbeSynthetic, health = ProbeUnknown, lastUpdate = Some(timestamp), lastChange = Some(timestamp))
    } else probe.getProbeStatus
    Some(EventEffect(status, Vector.empty))
  }

  /*
   * if the set of direct children has changed, or the probe policy has updated,
   * then update our state.
   */
  def update(probe: ProbeInterface, processor: BehaviorProcessor): Option[EventEffect] = {
    (children.keySet -- probe.children).foreach { ref => children.remove(ref)}
    (probe.children -- children.keySet).foreach { ref => children.put(ref, None)}
    None
  }

  /* ignore probe evaluations from client */
  def processEvaluation(probe: ProbeInterface, command: ProcessProbeEvaluation): Try[CommandEffect] = Failure(ApiException(BadRequest))

  /* process the status of a child probe */
  def processChild(probe: ProbeInterface, childRef: ProbeRef, childStatus: ProbeStatus): Option[EventEffect] = {
    children.put(childRef, Some(childStatus))

    val timestamp = DateTime.now(DateTimeZone.UTC)
    val health = evaluation.evaluate(children)
    val correlationId = if (health == ProbeHealthy) None else {
      if (probe.correlationId.isDefined) probe.correlationId else Some(UUID.randomUUID())
    }
    val lastUpdate = Some(timestamp)
    val lastChange = if (health == probe.health) probe.lastChange else {
      flapQueue.foreach(_.push(timestamp))
      Some(timestamp)
    }

    // we are healthy
    val acknowledgementId = if (health == ProbeHealthy) {
      probe.alertTimer.stop()
      None
    }
    // we are non-healthy
    else {
      if (!probe.alertTimer.isRunning)
        probe.alertTimer.start(probe.policy.alertTimeout)
      probe.acknowledgementId
    }

    val status = ProbeStatus(timestamp, probe.lifecycle, None, health, Map.empty, lastUpdate, lastChange, correlationId, acknowledgementId, probe.squelch)

    var notifications = Vector.empty[ProbeNotification]
    // append health notification
    flapQueue match {
      case Some(flapDetector) if flapDetector.isFlapping =>
        notifications = notifications :+ NotifyHealthFlaps(probe.probeRef, timestamp, correlationId, flapDetector.flapStart)
      case _ if probe.health != health =>
        notifications = notifications :+ NotifyHealthChanges(probe.probeRef, timestamp, correlationId, probe.health, health)
      case _ => // do nothing
    }
    // append recovery notification
    if (health == ProbeHealthy && probe.acknowledgementId.isDefined)
      notifications = notifications :+ NotifyRecovers(probe.probeRef, timestamp, probe.correlationId.get, probe.acknowledgementId.get)
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
    Some(EventEffect(status, notifications))
  }

  def exit(probe: ProbeInterface): Option[EventEffect] = {
    // stop timers
    probe.alertTimer.stop()
    None
  }
}

class AggregateProbe extends ProbeBehaviorExtension {
  override def implement(properties: Map[String, String]): BehaviorProcessor = new AggregateProcessor(EvaluateWorst)
}