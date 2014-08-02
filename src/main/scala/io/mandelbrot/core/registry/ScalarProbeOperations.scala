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
import akka.pattern.pipe
import io.mandelbrot.core.tracking._
import org.joda.time.{DateTimeZone, DateTime}
import scala.util.{Success, Failure}
import java.util.UUID

import io.mandelbrot.core.message.StatusMessage
import io.mandelbrot.core.state.DeleteProbeState
import io.mandelbrot.core.notification._
import io.mandelbrot.core.registry.Probe.{SendNotifications, ProbeAlertTimeout, ProbeExpiryTimeout}
import io.mandelbrot.core.{Conflict, BadRequest, InternalError, ResourceNotFound, ApiException}

/**
 *
 */
trait ScalarProbeOperations extends ProbeFSM with Actor {

  // for ask pattern
  import context.dispatcher
  implicit val timeout: akka.util.Timeout

  onTransition {
    case _ -> ScalarProbeFSMState =>
  }

  when(ScalarProbeFSMState) {

    /*
     * if the set of direct children has changed, or the probe policy has changed, then
     * update our state.  note that this may cause a FSM state change.
     */
    case Event(update: UpdateProbe, _) =>
      updateProbe(update)

    /*
     * if we receive a status message while joining or known, then update probe state
     * and send notifications.  if the previous lifecycle was joining, then we move to
     * known.  if we transition from non-healthy to healthy, then we clear the correlation
     * and acknowledgement (if set).  if we transition from healthy to non-healthy, then
     * we set the correlation if it is different from the current correlation, and we start
     * the alert timer.
     */
    case Event(message: StatusMessage, ScalarProbeFSMState(_, flapQueue)) =>
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
        notifications :+ NotifyLifecycleChanges(probeRef, message.timestamp, oldLifecycle, lifecycle)
      // append health notification
      flapQueue match {
        case Some(flapDetector) if flapDetector.isFlapping =>
          notifications :+ NotifyHealthFlaps(probeRef, message.timestamp, correlationId, flapDetector.flapStart)
        case _ if oldHealth != health =>
          notifications :+ NotifyHealthChanges(probeRef, message.timestamp, correlationId, oldHealth, health)
        case _ =>
          notifications :+ NotifyHealthUpdates(probeRef, message.timestamp, correlationId, health)
      }
      // append recovery notification
      if (health == ProbeHealthy && oldAcknowledgement.isDefined)
        notifications :+ NotifyRecovers(probeRef, timestamp, oldCorrelation.get, oldAcknowledgement.get)
      // update state and send notifications
      val status = ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
      commitStatusAndNotify(status, notifications)
      stay()

    /*
     * if we haven't received a status message within the current expiry window, then update probe
     * state and send notifications.  probe health becomes unknown, and correlation is set if it is
     * different from the current correlation.  we restart the expiry timer, and we start the alert
     * timer if it is not already running.
     */
    case Event(ProbeExpiryTimeout, ScalarProbeFSMState(_, flapQueue)) =>
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
          notifications :+ NotifyHealthFlaps(probeRef, timestamp, correlationId, flapDetector.flapStart)
        case _ if oldHealth != health =>
          notifications :+ NotifyHealthChanges(probeRef, timestamp, correlationId, oldHealth, health)
        case _ =>
          notifications :+ NotifyHealthExpires(probeRef, timestamp, correlationId)
      }
      // update state and send notifications
      val status = ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
      commitStatusAndNotify(status, notifications)
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
      val status = ProbeStatus(probeRef, DateTime.now(DateTimeZone.UTC), lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
      sender() ! GetProbeStatusResult(query, status)
      stay()

    /*
     * acknowledge an unhealthy probe.
     */
    case Event(command: AcknowledgeProbe, _) =>
      correlationId match {
        case None =>
          sender() ! ProbeOperationFailed(command, new ApiException(ResourceNotFound))
        case Some(correlation) if acknowledgementId.isDefined =>
          sender() ! ProbeOperationFailed(command, new ApiException(Conflict))
        case Some(correlation) if correlation != command.correlationId =>
          sender() ! ProbeOperationFailed(command, new ApiException(BadRequest))
        case Some(correlation) =>
          val acknowledgement = UUID.randomUUID()
          val timestamp = DateTime.now(DateTimeZone.UTC)
          val correlation = correlationId.get
          acknowledgementId = Some(acknowledgement)
          val status = ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
          val notifications = Vector(NotifyAcknowledged(probeRef, timestamp, correlation, acknowledgement))
          // update state and send notifications
          commitStatusAndNotify(status, notifications).flatMap { committed =>
            // create a ticket to track the acknowledgement
            trackingService.ask(CreateTicket(acknowledgement, timestamp, probeRef, correlation)).map {
              case result: CreateTicketResult =>
                AcknowledgeProbeResult(command, acknowledgement)
              case failure: TrackingServiceOperationFailed =>
                ProbeOperationFailed(command, failure.failure)
            }.recover { case ex => ProbeOperationFailed(command, new ApiException(InternalError))}
          }.recover {
            case ex =>
              // FIXME: what is the impact on consistency if commit fails?
              log.error(ex, "failed to commit probe state")
              ProbeOperationFailed(command, ex)
          }.pipeTo(sender())
      }
      stay()

    /*
     * add a worknote to a ticket tracking an unhealthy probe.
     */
    case Event(command: AppendProbeWorknote, _) =>
      acknowledgementId match {
        case None =>
          sender() ! ProbeOperationFailed(command, new ApiException(ResourceNotFound))
        case Some(acknowledgement) if acknowledgement != command.acknowledgementId =>
          sender() ! ProbeOperationFailed(command, new ApiException(BadRequest))
        case Some(acknowledgement) =>
          val timestamp = DateTime.now(DateTimeZone.UTC)
          trackingService.tell(AppendWorknote(acknowledgement, timestamp, command.comment, command.internal.getOrElse(false)), sender())
      }
      stay()

    /*
     *
     */
    case Event(command: UnacknowledgeProbe, _) =>
      acknowledgementId match {
        case None =>
          sender() ! ProbeOperationFailed(command, new ApiException(ResourceNotFound))
        case Some(acknowledgement) if acknowledgement != command.acknowledgementId =>
          sender() ! ProbeOperationFailed(command, new ApiException(BadRequest))
        case Some(acknowledgement) =>
          val timestamp = DateTime.now(DateTimeZone.UTC)
          val correlation = correlationId.get
          acknowledgementId = None
          val status = ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
          val notifications = Vector(NotifyUnacknowledged(probeRef, timestamp, correlation, acknowledgement))
          // update state and send notifications
          commitStatusAndNotify(status, notifications).flatMap { committed =>
            // close the ticket
            trackingService.ask(CloseTicket(acknowledgement)).map {
              case result: CloseTicketResult =>
                UnacknowledgeProbeResult(command, acknowledgement)
              case failure: TrackingServiceOperationFailed =>
                ProbeOperationFailed(command, failure.failure)
            }.recover { case ex => ProbeOperationFailed(command, new ApiException(InternalError))}
          }.recover {
            case ex =>
              // FIXME: what is the impact on consistency if commit fails?
              log.error(ex, "failed to commit probe state")
              ProbeOperationFailed(command, ex)
          }.pipeTo(sender())
      }
      stay()

    /*
     *
     */
    case Event(command: SetProbeSquelch, _) =>
      if (squelch == command.squelch) {
        sender() ! ProbeOperationFailed(command, new ApiException(BadRequest))
      } else {
        val timestamp = DateTime.now(DateTimeZone.UTC)
        squelch = command.squelch
        if (command.squelch)
          sendNotification(NotifySquelched(probeRef, timestamp))
        else
          sendNotification(NotifyUnsquelched(probeRef, timestamp))
        // reply to sender
        sender() ! SetProbeSquelchResult(command, squelch)
      }
      stay()

    /*
     * send a batch of notifications, adhering to the current notification policy.
     */
    case Event(SendNotifications(notifications), _) =>
      notifications.foreach(sendNotification)
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
      stateService.ask(DeleteProbeState(probeRef, Some(status), lsn)).onComplete {
        case Success(committed) =>
          self ! SendNotifications(notifications)
          self ! PoisonPill
        // FIXME: what is the impact on consistency if commit fails?
        case Failure(ex) => log.error(ex, "failed to commit probe state")
      }
      stay()
  }
}

case class ScalarProbeFSMState(behavior: ScalarBehaviorPolicy,
                               flapQueue: Option[FlapQueue]) extends ProbeFSMData

case object ScalarProbeFSMState extends ProbeFSMState {
  def apply(behavior: ScalarBehaviorPolicy): ScalarProbeFSMState = {
    ScalarProbeFSMState(behavior, None)
  }
  def apply(behavior: ScalarBehaviorPolicy, oldState: ScalarProbeFSMState): ScalarProbeFSMState = {
    ScalarProbeFSMState(behavior, None)
  }
}
