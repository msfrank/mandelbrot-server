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

import akka.actor._
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.duration._
import scala.collection.mutable
import java.util.UUID

import io.mandelbrot.core.{ResourceNotFound, Conflict, BadRequest, InternalError, ApiException}
import io.mandelbrot.core.message._
import io.mandelbrot.core.state._
import io.mandelbrot.core.notification._
import io.mandelbrot.core.tracking._
import scala.util.{Failure, Success}

/**
 * the Probe actor encapsulates all of the monitoring business logic.  For every probe
 * declared by an agent or proxy, there is a corresponding Probe actor.
 */
class Probe(probeRef: ProbeRef,
            parent: ActorRef,
            var children: Set[ProbeRef],
            var policy: ProbePolicy,
            generation: Long,
            stateService: ActorRef,
            notificationService: ActorRef,
            trackingService: ActorRef) extends Actor with Stash with ActorLogging {
  import Probe._
  import context.dispatcher

  // config
  implicit val timeout: Timeout = 5.seconds   // TODO: pull this from settings

  // state
  var lifecycle: ProbeLifecycle = ProbeJoining
  var health: ProbeHealth = ProbeUnknown
  var summary: Option[String] = None
  var lastChange: Option[DateTime] = None
  var lastUpdate: Option[DateTime] = None
  var correlationId: Option[UUID] = None
  var acknowledgementId: Option[UUID] = None
  var squelch: Boolean = false

  var notifier: Option[ActorRef] = None
  var flapQueue: Option[FlapQueue] = None
  var escalationMap = new mutable.HashMap[ProbeRef,Option[ProbeStatus]]
  var expiryTimer = new Timer(context, self, ProbeExpiryTimeout)
  var alertTimer = new Timer(context, self, ProbeAlertTimeout)

  override def preStart(): Unit = {
    // set the initial policy
    applyPolicy(policy)
    // initialize the escalation map
    children.foreach { ref => escalationMap.put(ref, None) }
    // ask state service what our current status is
    stateService.ask(InitializeProbeState(probeRef, DateTime.now(DateTimeZone.UTC), generation)).map {
      case result @ Success(state: ProbeState) =>
        log.debug("gen {}: received initial status from state service: {} (lsn {})", generation, state.status, state.lsn)
        result
      case result @ Failure(failure: Throwable) =>
        log.debug("gen {}: failure receiving initial state from state service: {}", generation, failure)
        result
    }.pipeTo(self)
  }

  override def postStop(): Unit = {
    expiryTimer.stop()
    alertTimer.stop()
  }

  def receive = {

    /*
     *
     */
    case Success(ProbeState(status, lsn)) =>
      // initialize probe state
      lifecycle = status.lifecycle
      health = status.health
      summary = status.summary
      lastChange = status.lastChange
      lastUpdate = status.lastUpdate
      correlationId = status.correlation
      acknowledgementId = status.acknowledged
      squelch = status.squelched
      // this generation is not current, so switch to retired behavior
      if (lsn > generation) {
        context.become(retired)
        log.debug("probe {} becomes retired (lsn {})", probeRef, lsn)
        unstashAll()
      }
      // otherwise switch to running behavior and replay any stashed messages
      else {
        context.become(running)
        log.debug("probe {} becomes running (lsn {})", probeRef, lsn)
        unstashAll()
        // start the expiry timer using the joining timeout
        resetExpiryTimer()
      }

    case Failure(failure: ApiException) if failure.failure == ResourceNotFound =>
      context.become(retired)
      log.debug("probe {} becomes retired", probeRef)
      unstashAll()

    case Failure(failure: Throwable) =>
      throw failure

    case other =>
      stash()
  }

  def retired: Receive = {

    case RetireProbe(lsn) =>
      context.stop(self)

    case _ =>
      // ignore any other message
  }

  def running: Receive = {

    /*
     * if the set of direct children has changed, and we are rolling up alerts, then
     * update our escalation state.  if the probe policy has changed, then update any
     * running timers.
     */
    case UpdateProbe(directChildren, newPolicy, lsn) =>
      applyPolicy(newPolicy)
      (children -- directChildren).foreach { ref => escalationMap.remove(ref) }
      (directChildren -- children).foreach { ref => escalationMap.put(ref, None) }
      children = directChildren
      resetExpiryTimer()
      // FIXME: reset alert timer as well?

    /*
     * if we receive a status message while joining or known, then update probe state
     * and send notifications.  if the previous lifecycle was joining, then we move to
     * known.  if we transition from non-healthy to healthy, then we clear the correlation
     * and acknowledgement (if set).  if we transition from healthy to non-healthy, then
     * we set the correlation if it is different from the current correlation, and we start
     * the alert timer.
     */
    case message: StatusMessage =>
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

    /*
     * if we haven't received a status message within the current expiry window, then update probe
     * state and send notifications.  probe health becomes unknown, and correlation is set if it is
     * different from the current correlation.  we restart the expiry timer, and we start the alert
     * timer if it is not already running.
     */
    case ProbeExpiryTimeout =>
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

    /*
     * if the alert timer expires, then send a health-alerts notification and restart the alert timer.
     */
    case ProbeAlertTimeout =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      // restart the alert timer
      alertTimer.restart(policy.alertTimeout)
      // send alert notification
      correlationId match {
        case Some(correlation) =>
          sendNotification(NotifyHealthAlerts(probeRef, timestamp, health, correlation, acknowledgementId))
        case None =>  // do nothing
      }

    /*
     * retrieve the status of the probe.
     */
    case query: GetProbeStatus =>
      val status = ProbeStatus(probeRef, DateTime.now(DateTimeZone.UTC), lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
      sender() ! GetProbeStatusResult(query, status)

    /*
     * acknowledge an unhealthy probe.
     */
    case command: AcknowledgeProbe =>
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
          stateService.ask(ProbeState(status, generation)).flatMap { committed =>
            // send notifications once status has been committed
            self ! SendNotifications(notifications)
            // create a ticket to track the acknowledgement
            trackingService.ask(CreateTicket(acknowledgement, timestamp, probeRef, correlation)).map {
              case result: CreateTicketResult =>
                AcknowledgeProbeResult(command, acknowledgement)
              case failure: TrackingServiceOperationFailed =>
                ProbeOperationFailed(command, failure.failure)
            }.recover { case ex => ProbeOperationFailed(command, new ApiException(InternalError)) }
          }.recover {
            case ex =>
              // FIXME: what is the impact on consistency if commit fails?
              log.error(ex, "failed to commit probe state")
              ProbeOperationFailed(command, ex)
          }.pipeTo(sender())

      }

    /*
     * add a worknote to a ticket tracking an unhealthy probe.
     */
    case command: AppendProbeWorknote =>
      acknowledgementId match {
        case None =>
          sender() ! ProbeOperationFailed(command, new ApiException(ResourceNotFound))
        case Some(acknowledgement) if acknowledgement != command.acknowledgementId =>
          sender() ! ProbeOperationFailed(command, new ApiException(BadRequest))
        case Some(acknowledgement) =>
          val timestamp = DateTime.now(DateTimeZone.UTC)
          trackingService.tell(AppendWorknote(acknowledgement, timestamp, command.comment, command.internal.getOrElse(false)), sender())
      }

    /*
     *
     */
    case command: UnacknowledgeProbe =>
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
        stateService.ask(ProbeState(status, generation)).flatMap { committed =>
          // TODO: send unacknowledgement notification once status has been committed
          // close the ticket
          trackingService.ask(CloseTicket(acknowledgement)).map {
            case result: CloseTicketResult =>
              UnacknowledgeProbeResult(command, acknowledgement)
            case failure: TrackingServiceOperationFailed =>
              ProbeOperationFailed(command, failure.failure)
          }.recover { case ex => ProbeOperationFailed(command, new ApiException(InternalError)) }
        }.recover {
          case ex =>
            // FIXME: what is the impact on consistency if commit fails?
            log.error(ex, "failed to commit probe state")
            ProbeOperationFailed(command, ex)
        }.pipeTo(sender())
      }

    /*
     *
     */
    case command: SetProbeSquelch =>
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

    /*
     * send a batch of notifications, adhering to the current notification policy.
     */
    case SendNotifications(notifications) =>
      notifications.foreach(sendNotification)

    /*
     * forward all received notifications upstream.
     */
    case notification: Notification =>
      // FIXME: this should probably be removed once escalation policy is implemented.
      notifier.foreach(_ ! notification)

    /*
     * probe lifecycle is leaving and the leaving timeout has expired.  probe lifecycle is set to
     * retired, state is updated, and lifecycle-changes notification is sent.  finally, all timers
     * are stopped, then the actor itself is stopped.
     */
    case RetireProbe(lsn) =>
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
  }

  /**
   * send the notification if the notification set policy is not specified (meaning
   * send all notifications) or if the policy is specified and this specific notification
   * type is in the notification set.
   */
  def sendNotification(notification: Notification): Unit = notifier.foreach { _notifier =>
    if (policy.notificationPolicy.notifications.isEmpty)
        _notifier ! notification
    else if (policy.notificationPolicy.notifications.get.contains(notification.kind))
        _notifier ! notification
  }

  /**
   * send probe status to the state service, and wait for acknowledgement.  if update is
   * acknowledged then send notifications, otherwise log an error.
   */
  def commitStatusAndNotify(status: ProbeStatus, notifications: Vector[Notification]): Unit = {
    stateService.ask(ProbeState(status, generation)).onComplete {
      case Success(committed) => self ! SendNotifications(notifications)
      // FIXME: what is the impact on consistency if commit fails?
      case Failure(ex) => log.error(ex, "failed to commit probe state")
    }
  }

  /**
   * reset the expiry timer, checking lastTimeout.  this will potentially send
   * a ProbeExpiryTimeout message if the timeout from a new policy is smaller than
   * the old policy.
   */
  def resetExpiryTimer(): Unit = {
    lifecycle match {
      case ProbeJoining =>
        expiryTimer.reset(policy.joiningTimeout)
      case ProbeKnown =>
        expiryTimer.reset(policy.probeTimeout)
      case ProbeRetired =>
        throw new Exception("resetting expiry timer for retired probe")
    }
  }

  /**
   * restart the expiry timer, but don't check lastTimeout when re-arming, otherwise
   * we may get duplicate ProbeExpiryTimeout messages.
   */
  def restartExpiryTimer(): Unit = {
    lifecycle match {
      case ProbeJoining =>
        expiryTimer.restart(policy.joiningTimeout)
      case ProbeKnown =>
        expiryTimer.restart(policy.probeTimeout)
      case ProbeRetired =>
        throw new Exception("restarting expiry timer for retired probe")
    }
  }

  /**
   * update state based on the new policy.
   */
  def applyPolicy(newPolicy: ProbePolicy): Unit = {
    log.debug("probe {} updates configuration: {}", probeRef, newPolicy)
    newPolicy.notificationPolicy.behavior match {
      case EscalateNotifications =>
        notifier = Some(parent)
      case EmitNotifications =>
        notifier = Some(notificationService)
      case SquelchNotifications =>
        notifier = None
    }
    policy = newPolicy
  }
}

object Probe {
  def props(probeRef: ProbeRef,
            parent: ActorRef,
            children: Set[ProbeRef],
            policy: ProbePolicy,
            generation: Long,
            stateService: ActorRef,
            notificationService: ActorRef,
            trackingService: ActorRef) = {
    Props(classOf[Probe], probeRef, parent, children, policy, generation, stateService, notificationService, trackingService)
  }

  case class SendNotifications(notifications: Vector[Notification])
  case object ProbeAlertTimeout
  case object ProbeExpiryTimeout
}

/* object lifecycle */
sealed trait ProbeLifecycle
case object ProbeJoining extends ProbeLifecycle { override def toString = "joining" }
case object ProbeKnown extends ProbeLifecycle   { override def toString = "known" }
case object ProbeRetired extends ProbeLifecycle { override def toString = "retired" }

/* object state */
sealed trait ProbeHealth
case object ProbeHealthy extends ProbeHealth  { override def toString = "healthy" }
case object ProbeDegraded extends ProbeHealth { override def toString = "degraded" }
case object ProbeFailed extends ProbeHealth   { override def toString = "failed" }
case object ProbeUnknown extends ProbeHealth  { override def toString = "unknown" }

/* */
case class ProbeStatus(probeRef: ProbeRef,
                       timestamp: DateTime,
                       lifecycle: ProbeLifecycle,
                       health: ProbeHealth,
                       summary: Option[String],
                       lastUpdate: Option[DateTime],
                       lastChange: Option[DateTime],
                       correlation: Option[UUID],
                       acknowledged: Option[UUID],
                       squelched: Boolean)

case class ProbeMetadata(probeRef: ProbeRef, metadata: Map[String,String])

/* */
sealed trait ProbeOperation { val probeRef: ProbeRef }
sealed trait ProbeCommand extends ProbeOperation
sealed trait ProbeQuery extends ProbeOperation
case class ProbeOperationFailed(op: ProbeOperation, failure: Throwable)

case class GetProbeStatus(probeRef: ProbeRef) extends ProbeQuery
case class GetProbeStatusResult(op: GetProbeStatus, state: ProbeStatus)

case class SetProbeSquelch(probeRef: ProbeRef, squelch: Boolean) extends ProbeCommand
case class SetProbeSquelchResult(op: SetProbeSquelch, squelch: Boolean)

case class AcknowledgeProbe(probeRef: ProbeRef, correlationId: UUID) extends ProbeCommand
case class AcknowledgeProbeResult(op: AcknowledgeProbe, acknowledgementId: UUID)

case class AppendProbeWorknote(probeRef: ProbeRef, acknowledgementId: UUID, comment: String, internal: Option[Boolean]) extends ProbeCommand
case class AppendProbeWorknoteResult(op: AppendProbeWorknote, worknoteId: UUID)

case class UnacknowledgeProbe(probeRef: ProbeRef, acknowledgementId: UUID) extends ProbeCommand
case class UnacknowledgeProbeResult(op: UnacknowledgeProbe, acknowledgementId: UUID)
