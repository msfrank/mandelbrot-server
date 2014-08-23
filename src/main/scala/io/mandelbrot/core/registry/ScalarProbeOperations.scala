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

package io.mandelbrot.core.registry

import akka.actor.{Actor, PoisonPill}
import akka.pattern.ask
import io.mandelbrot.core.system.{StatusMessage, FlapQueue}
import org.joda.time.{DateTimeZone, DateTime}
import scala.util.{Success, Failure}
import java.util.UUID
import io.mandelbrot.core.state.DeleteProbeState
import io.mandelbrot.core.notification._
import io.mandelbrot.core.registry.Probe.{SendNotifications, ProbeAlertTimeout, ProbeExpiryTimeout}

/**
 *
 */
trait ScalarProbeOperations extends ProbeFSM with Actor {

  // for ask pattern
  import context.dispatcher
  implicit val timeout: akka.util.Timeout

  onTransition {
    case _ -> ScalarProbeFSMState =>
      expiryTimer.restart(policy.joiningTimeout)
      if (lifecycle == ProbeInitializing) {
        lifecycle = ProbeJoining
        health = ProbeUnknown
        val timestamp = DateTime.now(DateTimeZone.UTC)
        lastChange = Some(timestamp)
        lastUpdate = Some(timestamp)
      }
  }

  when(ScalarProbeFSMState) {

    /* if the probe behavior has changed, then transition to a new state */
    case Event(change: ChangeProbe, _) =>
      goto(ChangingProbeFSMState) using ChangingProbeFSMState(change)

    /*
     * if the set of direct children has changed, or the probe policy has updated,
     * then update our state.
     */
    case Event(update: UpdateProbe, data: ScalarProbeFSMState) =>
      children = update.children
      policy = update.policy
      stay() using data.update(update)

    /*
     * if we receive a status message while joining or known, then update probe state
     * and send notifications.  if the previous lifecycle was joining, then we move to
     * known.  if we transition from non-healthy to healthy, then we clear the correlation
     * and acknowledgement (if set).  if we transition from healthy to non-healthy, then
     * we set the correlation if it is different from the current correlation, and we start
     * the alert timer.
     */
    case Event(message: StatusMessage, ScalarProbeFSMState(flapQueue)) =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val correlation = if (message.health == ProbeHealthy) None else {
        if (correlationId.isDefined) correlationId else Some(UUID.randomUUID())
      }
      summary = Some(message.summary)
      lastUpdate = Some(timestamp)
      val oldLifecycle = lifecycle
      val oldHealth = health
      val oldCorrelation = correlationId
      val oldAcknowledgement = acknowledgementId
      // update lifecycle
      if (oldLifecycle == ProbeJoining)
        lifecycle = ProbeKnown
      // reset the expiry timer
      resetExpiryTimer()
      // update health
      health = message.health
      if (health != oldHealth) {
        lastChange = Some(timestamp)
        flapQueue.foreach(_.push(message.timestamp))
      }
      // we are healthy
      if (health == ProbeHealthy) {
        correlationId = None
        acknowledgementId = None
        alertTimer.stop()
      }
      // we are non-healthy
      else {
        if (correlationId != correlation)
          correlationId = correlation
        if (!alertTimer.isRunning)
          alertTimer.start(policy.alertTimeout)
      }
      var notifications = Vector.empty[Notification]
      // append lifecycle notification
      if (lifecycle != oldLifecycle)
        notifications = notifications :+ NotifyLifecycleChanges(probeRef, message.timestamp, oldLifecycle, lifecycle)
      // append health notification
      flapQueue match {
        case Some(flapDetector) if flapDetector.isFlapping =>
          notifications = notifications :+ NotifyHealthFlaps(probeRef, message.timestamp, correlationId, flapDetector.flapStart)
        case _ if oldHealth != health =>
          notifications = notifications :+ NotifyHealthChanges(probeRef, message.timestamp, correlationId, oldHealth, health)
        case _ =>
          notifications = notifications :+ NotifyHealthUpdates(probeRef, message.timestamp, correlationId, health)
      }
      // append recovery notification
      if (health == ProbeHealthy && oldAcknowledgement.isDefined)
        notifications = notifications :+ NotifyRecovers(probeRef, timestamp, oldCorrelation.get, oldAcknowledgement.get)
      // update state and send notifications
      commitStatusAndNotify(getProbeStatus(timestamp), notifications)
      stay()

    /*
     * if we haven't received a status message within the current expiry window, then update probe
     * state and send notifications.  probe health becomes unknown, and correlation is set if it is
     * different from the current correlation.  we restart the expiry timer, and we start the alert
     * timer if it is not already running.
     */
    case Event(ProbeExpiryTimeout, ScalarProbeFSMState(flapQueue)) =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val correlation = if (correlationId.isDefined) correlationId else Some(UUID.randomUUID())
      val oldHealth = health
      // update health
      health = ProbeUnknown
      summary = None
      if (health != oldHealth) {
        lastChange = Some(timestamp)
        flapQueue.foreach(_.push(timestamp))
      }
      // we transition from healthy to non-healthy
      if (correlationId != correlation)
        correlationId = correlation
      if (!alertTimer.isRunning)
        alertTimer.start(policy.alertTimeout)
      // reset the expiry timer
      restartExpiryTimer()
      var notifications = Vector.empty[Notification]
      // append health notification
      flapQueue match {
        case Some(flapDetector) if flapDetector.isFlapping =>
          notifications = notifications :+ NotifyHealthFlaps(probeRef, timestamp, correlationId, flapDetector.flapStart)
        case _ if oldHealth != health =>
          notifications = notifications :+ NotifyHealthChanges(probeRef, timestamp, correlationId, oldHealth, health)
        case _ =>
          notifications = notifications :+ NotifyHealthExpires(probeRef, timestamp, correlationId)
      }
      // update state and send notifications
      commitStatusAndNotify(getProbeStatus(timestamp), notifications)
      stay()

    /*
     * if the alert timer expires, then send a health-alerts notification and restart the alert timer.
     */
    case Event(ProbeAlertTimeout, _) =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      // restart the alert timer
      alertTimer.restart(policy.alertTimeout)
      // send alert notification
      correlationId match {
        case Some(correlation) =>
          sendNotification(NotifyHealthAlerts(probeRef, timestamp, health, correlation, acknowledgementId))
        case None => // do nothing
      }
      stay()

    /*
     * retrieve the status of the probe.
     */
    case Event(query: GetProbeStatus, _) =>
      sender() ! GetProbeStatusResult(query, getProbeStatus)
      stay()

    /*
     * acknowledge an unhealthy probe.
     */
    case Event(command: AcknowledgeProbe, _) =>
      acknowledgeProbe(command, sender())
      stay()

    /*
     * add a worknote to a ticket tracking an unhealthy probe.
     */
    case Event(command: AppendProbeWorknote, _) =>
      appendComment(command, sender())
      stay()

    /*
     * remove the acknowledgement from a probe.
     */
    case Event(command: UnacknowledgeProbe, _) =>
      unacknowledgeProbe(command, sender())
      stay()

    /*
     * squelch or unsquelch all probe notifications.
     */
    case Event(command: SetProbeSquelch, _) =>
      squelchProbe(command, sender())
      stay()

    /*
     * send a batch of notifications, adhering to the current notification policy.
     */
    case Event(SendNotifications(notifications), _) =>
      sendNotifications(notifications)
      stay()

    /*
     * forward all alerts up to the parent.
     */
    case Event(alert: Alert, _) =>
      sendNotification(alert)
      stay()

    /*
     * probe lifecycle is leaving and the leaving timeout has expired.  probe lifecycle is set to
     * retired, state is updated, and lifecycle-changes notification is sent.  finally, all timers
     * are stopped, then the actor itself is stopped.
     */
    case Event(RetireProbe(lsn), _) =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val oldLifecycle = lifecycle
      lifecycle = ProbeRetired
      summary = None
      lastUpdate = Some(timestamp)
      lastChange = Some(timestamp)
      val status = ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
      val notifications = Vector(NotifyLifecycleChanges(probeRef, timestamp, oldLifecycle, lifecycle))
      // stop timers
      expiryTimer.stop()
      alertTimer.stop()
      // update state
      services.stateService.ask(DeleteProbeState(probeRef, Some(status), lsn)).onComplete {
        case Success(committed) =>
          self ! SendNotifications(notifications)
          self ! PoisonPill
        // FIXME: what is the impact on consistency if commit fails?
        case Failure(ex) => log.error(ex, "failed to commit probe state")
      }
      stay()
  }
}

case class ScalarProbeFSMState(flapQueue: Option[FlapQueue]) extends ProbeFSMData {
  def update(update: UpdateProbe): ScalarProbeFSMState = {
    this
  }
}

case object ScalarProbeFSMState extends ProbeFSMState {
  def apply(behavior: ScalarBehaviorPolicy): ScalarProbeFSMState = {
    ScalarProbeFSMState(None)
  }
}
