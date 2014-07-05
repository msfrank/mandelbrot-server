package io.mandelbrot.core.registry

import akka.actor.{Actor, PoisonPill}
import akka.pattern.ask
import akka.pattern.pipe
import io.mandelbrot.core.tracking._
import org.joda.time.{DateTimeZone, DateTime}
import java.util.UUID

import io.mandelbrot.core.message.StatusMessage
import io.mandelbrot.core.notification._
import io.mandelbrot.core.registry.Probe.{SendNotifications, ProbeAlertTimeout, ProbeExpiryTimeout}
import io.mandelbrot.core.state.{ProbeState, DeleteProbeState}
import io.mandelbrot.core.{Conflict, BadRequest, InternalError, ResourceNotFound, ApiException}

import scala.util.{Success, Failure}

/**
 *
 */
trait ScalarProbeOperations extends ProbeFSM with Actor {

  // for ask pattern
  import context.dispatcher
  implicit val timeout: akka.util.Timeout

  when(ScalarProbeFSMState) {

    /*
     * if the set of direct children has changed, and we are rolling up alerts, then
     * update our escalation state.  if the probe policy has changed, then update any
     * running timers.
     */
    case Event(UpdateProbe(directChildren, newPolicy, lsn), _) =>
      applyPolicy(newPolicy)
      children = directChildren
      resetExpiryTimer()
      // FIXME: reset alert timer as well?
      stay() using ScalarProbeFSMState()

    /*
     * if we receive a status message while joining or known, then update probe state
     * and send notifications.  if the previous lifecycle was joining, then we move to
     * known.  if we transition from non-healthy to healthy, then we clear the correlation
     * and acknowledgement (if set).  if we transition from healthy to non-healthy, then
     * we set the correlation if it is different from the current correlation, and we start
     * the alert timer.
     */
    case Event(message: StatusMessage, _) =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val correlation = if (message.health == ProbeHealthy) None
      else {
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
    case Event(ProbeExpiryTimeout, _) =>
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
          // update state
          stateService.ask(ProbeState(status, probeGeneration)).flatMap { committed =>
            // send notifications once status has been committed
            self ! SendNotifications(notifications)
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
          acknowledgementId = None
          val status = ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
          // update state
          stateService.ask(ProbeState(status, probeGeneration)).flatMap { committed =>
            // TODO: send unacknowledgement notification once status has been committed
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
     * forward all received notifications upstream.
     */
    case Event(notification: Notification, _) =>
      // FIXME: this should probably be removed once escalation policy is implemented.
      notifier.foreach(_ ! notification)
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

case object ScalarProbeFSMState extends ProbeFSMState
case class ScalarProbeFSMState() extends ProbeFSMData
