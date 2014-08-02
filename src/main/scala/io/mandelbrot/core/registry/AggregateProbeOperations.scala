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

import java.util.UUID

import akka.actor.{PoisonPill, Actor}
import akka.pattern.ask
import org.joda.time.{DateTimeZone, DateTime}
import scala.collection.mutable
import scala.util.{Failure, Success}

import io.mandelbrot.core.registry.Probe.{SendNotifications, ProbeAlertTimeout, ProbeExpiryTimeout}
import io.mandelbrot.core.notification._
import io.mandelbrot.core.message.StatusMessage
import io.mandelbrot.core.state.DeleteProbeState

/**
 *
 */
trait AggregateProbeOperations extends ProbeFSM with Actor {

  // for ask pattern
  import context.dispatcher
  implicit val timeout: akka.util.Timeout

  onTransition {
    case _ -> AggregateProbeFSMState =>
  }

  when(AggregateProbeFSMState) {

    /*
     * if the set of direct children has changed, or the probe policy has changed, then
     * update our state.  note that this may cause a FSM state change.
     */
    case Event(command: UpdateProbe, _) =>
      updateProbe(command)

    /* ignore status from any probe which is not a direct child */
    case Event(childStatus: ProbeStatus, data: AggregateProbeFSMState) if !data.children.contains(childStatus.probeRef) =>
      stay()

    /* */
    case Event(childStatus: ProbeStatus, AggregateProbeFSMState(_, statusMap, flapQueue)) =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      statusMap.put(childStatus.probeRef, Some(childStatus))
      // update lifecycle
      val oldLifecycle = lifecycle
      if (oldLifecycle != ProbeKnown)
        lifecycle = statusMap.values.foldLeft[ProbeLifecycle](ProbeKnown) { case (s, o) => if (o.isEmpty) ProbeJoining else s }
      val newHealth = if (lifecycle == ProbeKnown) findWorstStatus(statusMap) else ProbeUnknown
      val correlation = if (newHealth == ProbeHealthy) None else {
        if (correlationId.isDefined) correlationId else Some(UUID.randomUUID())
      }
      lastUpdate = Some(timestamp)
      val oldHealth = health
      val oldCorrelation = correlationId
      val oldAcknowledgement = acknowledgementId
      // reset the expiry timer
      resetExpiryTimer()
      // update health
      health = newHealth
      if (health != oldHealth) {
        lastChange = Some(timestamp)
        flapQueue.foreach(_.push(timestamp))
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
        notifications = notifications :+ NotifyLifecycleChanges(probeRef, timestamp, oldLifecycle, lifecycle)
      // append health notification
      flapQueue match {
        case Some(flapDetector) if flapDetector.isFlapping =>
          notifications = notifications :+ NotifyHealthFlaps(probeRef, timestamp, correlationId, flapDetector.flapStart)
        case _ if oldHealth != health =>
          notifications = notifications :+ NotifyHealthChanges(probeRef, timestamp, correlationId, oldHealth, health)
        case _ => // do nothing
      }
      // append recovery notification
      if (health == ProbeHealthy && oldAcknowledgement.isDefined)
        notifications = notifications :+ NotifyRecovers(probeRef, timestamp, oldCorrelation.get, oldAcknowledgement.get)
      // if state has changed, update state and send notifications, otherwise just send notifications
      if (lifecycle != oldLifecycle || health != oldHealth)
        commitStatusAndNotify(getProbeStatus(timestamp), notifications)
      else
        sendNotifications(notifications)
      stay()


    /* ignore spurious StatusMessage messages */
    case Event(message: StatusMessage, _) =>
      stay()

    /* ignore spurious ProbeExpiryTimeout messages */
    case Event(ProbeExpiryTimeout, _) =>
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
      notifications.foreach(sendNotification)
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
      stateService.ask(DeleteProbeState(probeRef, Some(status), lsn)).onComplete {
        case Success(committed) =>
          self ! SendNotifications(notifications)
          self ! PoisonPill
        // FIXME: what is the impact on consistency if commit fails?
        case Failure(ex) => log.error(ex, "failed to commit probe state")
      }
      stay()
  }

  /**
   * evaluate the status of each child probe, and return the worst one (where the order
   * of severity is defined as Unknown, Failed, Degraded, Healthy).
   */
  def findWorstStatus(childStatus: mutable.HashMap[ProbeRef,Option[ProbeStatus]]) = childStatus.values.foldLeft[ProbeHealth](ProbeHealthy) {
    case (ProbeUnknown, _) => ProbeUnknown
    case (curr, None) => ProbeUnknown
    case (curr, Some(next)) =>
      next.health match {
        case ProbeFailed if curr != ProbeUnknown => next.health
        case ProbeDegraded if curr != ProbeUnknown && curr != ProbeFailed => next.health
        case ProbeHealthy if curr != ProbeUnknown && curr != ProbeFailed && curr != ProbeDegraded => next.health
        case _ => curr
      }
  }

}

case class AggregateProbeFSMState(behavior: AggregateBehaviorPolicy,
                                  children: mutable.HashMap[ProbeRef,Option[ProbeStatus]],
                                  flapQueue: Option[FlapQueue]) extends ProbeFSMData

case object AggregateProbeFSMState extends ProbeFSMState {
  def apply(behavior: AggregateBehaviorPolicy, children: Set[ProbeRef]): AggregateProbeFSMState = {
    AggregateProbeFSMState(behavior, new mutable.HashMap[ProbeRef,Option[ProbeStatus]] ++= children.map(_ -> None), None)
  }
  def apply(behavior: AggregateBehaviorPolicy, children: Set[ProbeRef], oldState: AggregateProbeFSMState): AggregateProbeFSMState = {
    (children -- oldState.children.keySet).foreach { ref => oldState.children.remove(ref)}
    (oldState.children.keySet -- children).foreach { ref => oldState.children.put(ref, None)}
    AggregateProbeFSMState(behavior, oldState.children, None)
  }
}
