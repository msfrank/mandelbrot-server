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

import akka.actor.{ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import akka.persistence.{Recover, SnapshotOffer, EventsourcedProcessor}
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.duration._
import java.util.UUID

import io.mandelbrot.core.notification._
import io.mandelbrot.core.message.StatusMessage
import io.mandelbrot.core.{ResourceNotFound, Conflict, BadRequest, ApiException}
import io.mandelbrot.core.tracking.{TrackingServiceOperationFailed, CreateTicketResult, CreateTicket}

/**
 *
 */
class Probe(probeRef: ProbeRef,
            parent: ActorRef,
            stateService: ActorRef,
            notificationService: ActorRef,
            historyService: ActorRef,
            trackingService: ActorRef) extends EventsourcedProcessor with ActorLogging {
  import Probe._
  import ProbeSystem.{InitProbe,UpdateProbe,RetireProbe}
  import context.dispatcher

  // config
  override def processorId = probeRef.toString
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

  var currentPolicy: Option[ProbePolicy] = None
  var notifier: Option[ActorRef] = None
  var flapQueue: Option[FlapQueue] = None
  var expiryTimer = new Timer(context, self, ProbeExpiryTimeout)
  var alertTimer = new Timer(context, self, ProbeAlertTimeout)

  override def preStart(): Unit = {
    self ! Recover()
  }

  override def postStop(): Unit = {
    expiryTimer.stop()
    alertTimer.stop()
    //log.debug("snapshotting {}", processorId)
    //saveSnapshot(ProbeSnapshot(lifecycle, health, summary, detail, lastChange, lastUpdate, correlationId, acknowledgementId, squelch))
  }

  def receiveCommand = {

    case InitProbe(initialPolicy) =>
      persist(ProbeInitializes(initialPolicy, DateTime.now(DateTimeZone.UTC)))(updateState(_, recovering = false))

    case UpdateProbe(policy) =>
      if (currentPolicy.isDefined && policy != currentPolicy.get)
        persist(ProbeConfigures(policy, DateTime.now(DateTimeZone.UTC)))(updateState(_, recovering = false))

    case ProbeExpiryTimeout =>
      val correlation = if (health == ProbeHealthy) Some(UUID.randomUUID()) else None
      persist(ProbeExpires(correlation, DateTime.now(DateTimeZone.UTC)))(updateState(_, recovering = false))

    case ProbeAlertTimeout =>
      persist(ProbeAlerts(DateTime.now(DateTimeZone.UTC)))(updateState(_, recovering = false))

    case message: StatusMessage =>
      val correlation = if (health == ProbeHealthy && message.health != ProbeHealthy) Some(UUID.randomUUID()) else None
      persist(ProbeUpdates(message, correlation, DateTime.now(DateTimeZone.UTC)))(updateState(_, recovering = false))

    case query: GetProbeStatus =>
      val status = ProbeStatus(probeRef, DateTime.now(DateTimeZone.UTC), lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
      sender() ! GetProbeStatusResult(query, status)

    case command: AcknowledgeProbe =>
      correlationId match {
        case None =>
          sender() ! ProbeOperationFailed(command, new ApiException(ResourceNotFound))
        case Some(correlation) if acknowledgementId.isDefined =>
          sender() ! ProbeOperationFailed(command, new ApiException(Conflict))
        case Some(correlation) if correlation != command.correlationId =>
          log.debug("failed to acknowledge")
          sender() ! ProbeOperationFailed(command, new ApiException(BadRequest))
        case Some(correlation) =>
          val acknowledgement = UUID.randomUUID()
          val timestamp = DateTime.now(DateTimeZone.UTC)
          persist(UserAcknowledges(command, acknowledgement, timestamp))(updateState(_, recovering = false))
      }

    case command: SetProbeSquelch =>
      if (squelch == command.squelch) {
        sender() ! ProbeOperationFailed(command, new ApiException(BadRequest))
      } else {
        persist(UserSetsSquelch(command, DateTime.now(DateTimeZone.UTC)))(updateState(_, recovering = false))
      }

    case notification: Notification =>
      notifier.foreach(_ ! notification)

    case RetireProbe =>
      persist(ProbeRetires(DateTime.now(DateTimeZone.UTC)))(updateState(_, recovering = false))
  }

  def receiveRecover = {

    case event: Event =>
      updateState(event, recovering = true)

    case SnapshotOffer(metadata, snapshot: ProbeSnapshot) =>
      log.debug("loading snapshot of {} using offer {}", processorId, metadata)
      lifecycle = snapshot.lifecycle
      health = snapshot.health
      summary = snapshot.summary
      lastChange = snapshot.lastChange
      lastUpdate = snapshot.lastUpdate
      correlationId = snapshot.correlationId
      acknowledgementId = snapshot.acknowledgementId
      squelch = snapshot.squelch
  }

  def updateState(event: Event, recovering: Boolean) = event match {

    case ProbeInitializes(initialPolicy, timestamp) =>
      // set the initial policy
      updatePolicy(initialPolicy)
      resetExpiryTimer()
      // initialize probe state
      lifecycle = ProbeJoining
      summary = None
      lastUpdate = Some(timestamp)
      lastChange = Some(timestamp)
      if (!recovering) {
        // notify state service about updated state
        stateService ! ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
        // TODO: there should likely be a notification sent here
      }

    case ProbeConfigures(policy, timestamp) =>
      updatePolicy(policy)
      resetExpiryTimer()
      // TODO: there should likely be a notification sent here

    case ProbeUpdates(message, correlation, timestamp) =>
      summary = Some(message.summary)
      lastUpdate = Some(timestamp)
      val oldLifecycle = lifecycle
      val oldHealth = health
      // update lifecycle
      if (oldLifecycle == ProbeJoining)
        lifecycle = ProbeKnown
      // update health
      health = message.health
      if (health != oldHealth) {
        lastChange = Some(timestamp)
        flapQueue.foreach(_.push(message.timestamp))
        // we transition from healthy to non-healthy
        if (oldHealth == ProbeHealthy) {
          correlationId = correlation
          acknowledgementId = None
          alertTimer.start(currentPolicy.get.alertTimeout)
        }
        // we transition from non-healthy to healthy
        else if (health == ProbeHealthy) {
          correlationId = None
          acknowledgementId = None
          alertTimer.stop()
        }
      }
      if (!recovering) {
        // notify state service about updated state
        stateService ! ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
        // send lifecycle notifications
        if (lifecycle != oldLifecycle)
          notifier.foreach(_ ! NotifyLifecycleChanges(probeRef, message.timestamp, oldLifecycle, lifecycle))
        // send health notifications
        flapQueue match {
          case Some(flapDetector) if flapDetector.isFlapping =>
            sendNotification(NotifyHealthFlaps(probeRef, message.timestamp, correlationId, flapDetector.flapStart))
          case _ if oldHealth != health =>
            sendNotification(NotifyHealthChanges(probeRef, message.timestamp, correlationId, oldHealth, health))
          case _ =>
            sendNotification(NotifyHealthUpdates(probeRef, message.timestamp, correlationId, health))
        }
      }
      // reset the expiry timer
      resetExpiryTimer()

    case ProbeExpires(correlation, timestamp) =>
      val oldHealth = health
      // update health
      health = ProbeUnknown
      summary = None
      if (health != oldHealth) {
        lastChange = Some(timestamp)
        flapQueue.foreach(_.push(timestamp))
        // we transition from healthy to non-healthy
        if (oldHealth == ProbeHealthy) {
          correlationId = correlation
          acknowledgementId = None
          alertTimer.start(currentPolicy.get.alertTimeout)
        }
      }
      if (!recovering) {
        // notify state service if we transition to unknown
        if (health != oldHealth)
          stateService ! ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
        // send health notifications
        flapQueue match {
          case Some(flapDetector) if flapDetector.isFlapping =>
            sendNotification(NotifyHealthFlaps(probeRef, timestamp, correlationId, flapDetector.flapStart))
          case _ if oldHealth != health =>
            sendNotification(NotifyHealthChanges(probeRef, timestamp, correlationId, oldHealth, health))
          case _ =>
            sendNotification(NotifyHealthExpires(probeRef, timestamp, correlationId))
        }
      }
      // reset the expiry timer
      restartExpiryTimer()

    case ProbeAlerts(timestamp) =>
      // restart the alert timer
      alertTimer.restart(currentPolicy.get.alertTimeout)
      if (!recovering) {
        correlationId match {
          case Some(correlation) =>
            sendNotification(NotifyHealthAlerts(probeRef, timestamp, health, correlation, acknowledgementId))
          case None =>  // do nothing
        }
      }

    case UserAcknowledges(command, acknowledgement, timestamp) =>
      val correlation = correlationId.get
      acknowledgementId = Some(acknowledgement)
      if (!recovering) {
        // notify state service that we are acknowledged
        stateService ! ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
        // send acknowledgement notification
        sendNotification(NotifyAcknowledged(probeRef, timestamp, correlation, acknowledgement))
        // track the acknowledgement
        trackingService.ask(CreateTicket(acknowledgement, timestamp, probeRef, correlation)).map {
          case result: CreateTicketResult =>
            AcknowledgeProbeResult(command, acknowledgement)
          case failure: TrackingServiceOperationFailed =>
            ProbeOperationFailed(command, failure.failure)
        }.pipeTo(sender())
      }

    case UserSetsSquelch(command, timestamp) =>
      squelch = command.squelch
      if (!recovering) {
        if (command.squelch)
          sendNotification(NotifySquelched(probeRef, timestamp))
        else
          sendNotification(NotifyUnsquelched(probeRef, timestamp))
        // reply to sender
        sender() ! SetProbeSquelchResult(command, squelch)
      }

    case ProbeRetires(timestamp) =>
      val oldLifecycle = lifecycle
      lifecycle = ProbeRetired
      summary = None
      lastUpdate = Some(timestamp)
      lastChange = Some(timestamp)
      if (!recovering) {
        // notify state service about updated state
        // FIXME: delete probe state
        stateService ! ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
        // send lifecycle notifications
        sendNotification(NotifyLifecycleChanges(probeRef, timestamp, oldLifecycle, lifecycle))
      }
      // cancel timers
      expiryTimer.stop()
      alertTimer.stop()
  }

  /**
   * send the notification if the notification set policy is not specified (meaning
   * send all notifications) or if the policy is specified and this specific notification
   * type is in the notification set.
   */
  def sendNotification(notification: Notification): Unit = notifier.foreach { _notifier =>
    currentPolicy.foreach {
      case policy if policy.notificationPolicy.notifications.isEmpty =>
        _notifier ! notification
      case policy if policy.notificationPolicy.notifications.get.contains(notification.kind) =>
        _notifier ! notification
      case _ => // do nothing
    }
  }

  /**
   * reset the expiry timer, checking lastTimeout.  this will potentially send
   * a ProbeExpiryTimeout message if the timeout from a new policy is smaller than
   * the old policy.
   */
  def resetExpiryTimer(): Unit = {
    currentPolicy match {
      case Some(policy) =>
        lifecycle match {
          case ProbeJoining =>
            expiryTimer.reset(policy.joiningTimeout)
          case ProbeLeaving =>
            expiryTimer.reset(policy.leavingTimeout)
          case _ =>
            expiryTimer.reset(policy.probeTimeout)
        }
      case None => None   // FIXME: shouldn't ever happen, so throw exception here?
    }
  }

  /**
   * restart the expiry timer, but don't check lastTimeout when re-arming, otherwise
   * we may get duplicate ProbeExpiryTimeout messages.
   */
  def restartExpiryTimer(): Unit = {
    currentPolicy match {
      case Some(policy) =>
        lifecycle match {
          case ProbeJoining =>
            expiryTimer.restart(policy.joiningTimeout)
          case ProbeLeaving =>
            expiryTimer.restart(policy.leavingTimeout)
          case _ =>
            expiryTimer.restart(policy.probeTimeout)
        }
      case None => None   // FIXME: shouldn't ever happen, so throw exception here?
    }
  }

  /**
   * update state based on the new policy.
   */
  def updatePolicy(policy: ProbePolicy): Unit = {
    log.debug("probe {} updates configuration: {}", probeRef, policy)
    policy.notificationPolicy.behavior match {
      case EscalateNotifications =>
        notifier = Some(parent)
      case EmitNotifications =>
        notifier = Some(notificationService)
      case SquelchNotifications =>
        notifier = None
    }
    currentPolicy = Some(policy)
  }
}

object Probe {
  def props(probeRef: ProbeRef, parent: ActorRef, stateService: ActorRef, notificationService: ActorRef, historyService: ActorRef, trackingService: ActorRef) = {
    Props(classOf[Probe], probeRef, parent, stateService, notificationService, historyService, trackingService)
  }

  sealed trait Event
  case class ProbeInitializes(policy: ProbePolicy, timestamp: DateTime) extends Event
  case class ProbeConfigures(policy: ProbePolicy, timestamp: DateTime) extends Event
  case class ProbeAlerts(timestamp: DateTime) extends Event
  case class ProbeUpdates(state: StatusMessage, correlationId: Option[UUID], timestamp: DateTime) extends Event
  case class ProbeExpires(correlationId: Option[UUID], timestamp: DateTime) extends Event
  case class UserAcknowledges(op: AcknowledgeProbe, acknowledgementId: UUID, timestamp: DateTime) extends Event
  case class UserSetsSquelch(op: SetProbeSquelch, timestamp: DateTime) extends Event
  case class ProbeRetires(timestamp: DateTime) extends Event

  case object ProbeAlertTimeout
  case object ProbeExpiryTimeout

  case class ProbeSnapshot(lifecycle: ProbeLifecycle,
                           health: ProbeHealth,
                           summary: Option[String],
                           detail: Option[String],
                           lastChange: Option[DateTime],
                           lastUpdate: Option[DateTime],
                           correlationId: Option[UUID],
                           acknowledgementId: Option[UUID],
                           squelch: Boolean) extends Serializable
}

/* object lifecycle */
sealed trait ProbeLifecycle
case object ProbeJoining extends ProbeLifecycle { override def toString = "joining" }
case object ProbeKnown extends ProbeLifecycle   { override def toString = "known" }
case object ProbeLeaving extends ProbeLifecycle { override def toString = "leaving" }
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
