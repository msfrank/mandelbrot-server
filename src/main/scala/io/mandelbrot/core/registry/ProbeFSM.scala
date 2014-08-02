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

import akka.actor.{Actor, Stash, LoggingFSM, ActorRef}
import akka.pattern.ask
import akka.pattern.pipe
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.Future
import scala.util.{Failure, Success}

import io.mandelbrot.core.notification._
import io.mandelbrot.core.state.{ProbeStatusCommitted, ProbeState}
import io.mandelbrot.core.tracking._
import io.mandelbrot.core.{BadRequest, Conflict, ResourceNotFound, InternalError, ApiException}
import io.mandelbrot.core.registry.Probe.SendNotifications

/**
 * Base trait for the Probe FSM, containing the initializing and retiring logic,
 * as well as methods common to all Probe behaviors.
 */
trait ProbeFSM extends LoggingFSM[ProbeFSMState,ProbeFSMData] with Actor with Stash {

  // for ask pattern
  import context.dispatcher
  implicit val timeout: akka.util.Timeout

  // config
  val probeRef: ProbeRef
  val parent: ActorRef
  val probeGeneration: Long
  val stateService: ActorRef
  val notificationService: ActorRef
  val trackingService: ActorRef

  // state
  var children: Set[ProbeRef]
  var policy: ProbePolicy

  var lifecycle: ProbeLifecycle
  var health: ProbeHealth
  var summary: Option[String]
  var lastChange: Option[DateTime]
  var lastUpdate: Option[DateTime]
  var correlationId: Option[UUID]
  var acknowledgementId: Option[UUID]
  var squelch: Boolean

  var expiryTimer: Timer
  var alertTimer: Timer

  /**
   * wait for ProbeState from state service.  transition to Scalar or Aggregate state
   * (depending on the policy) if the lsn returned equals the probe generation, otherwise
   * transition directly to Retired.
   */
  when(InitializingProbeFSMState) {

    case Event(Success(ProbeState(status, lsn)), _) =>
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
      if (lsn > probeGeneration) {
        log.debug("probe {} becomes retired (lsn {})", probeRef, lsn)
        unstashAll()
        goto(RetiredProbeFSMState) using NoData
      }
      // otherwise replay any stashed messages and transition to initialized
      else {
        unstashAll()
        // start the expiry timer using the joining timeout
        resetExpiryTimer()
        // transition to next state depending on policy
        applyBehaviorPolicy(policy.behavior)
      }

    case Event(Failure(failure: ApiException), _) if failure.failure == ResourceNotFound =>
      log.debug("probe {} becomes retired", probeRef)
      unstashAll()
      goto(RetiredProbeFSMState) using NoData

    case Event(Failure(failure: Throwable), _) =>
      throw failure

    case other =>
      stash()
      stay()
  }

  /**
   * probe becomes Retired when it is determined to be stale; that is, the lsn from the
   * state service is newer than the probe generation.  when Retired, the probe ignores
   * all messages except for RetireProbe, which causes the Probe actor to stop.
   */
  when (RetiredProbeFSMState) {

    case Event(RetireProbe(lsn), _) =>
      context.stop(self)
      stay()

    // ignore any other message
    case _ =>
      stay()
  }

  /**
   * FIXME: this is a leaky abstraction
   */
  def applyBehaviorPolicy(behavior: BehaviorPolicy) = (policy.behavior,stateData) match {
    case (behavior: AggregateBehaviorPolicy, oldState: AggregateProbeFSMState) =>
      stay() using AggregateProbeFSMState(behavior, children, oldState)
    case (behavior: AggregateBehaviorPolicy, _) =>
      log.debug("probe {} becomes aggregate", probeRef)
      goto(AggregateProbeFSMState) using AggregateProbeFSMState(behavior, children)
    case (behavior: ScalarBehaviorPolicy, oldState: ScalarProbeFSMState) =>
      stay() using ScalarProbeFSMState(behavior, oldState)
    case (behavior: ScalarBehaviorPolicy, _) =>
      log.debug("probe {} becomes scalar", probeRef)
      goto(ScalarProbeFSMState) using ScalarProbeFSMState(behavior)
  }

  /**
   *
   */
  def updateProbe(update: UpdateProbe) = {
    children = update.children
    policy = update.policy
    log.debug("probe {} updates configuration: {}", probeRef, policy)
    resetExpiryTimer()
    // FIXME: reset alert timer as well?
    applyBehaviorPolicy(policy.behavior)
  }


  def getProbeStatus(timestamp: DateTime): ProbeStatus = {
    ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
  }

  def getProbeStatus: ProbeStatus = getProbeStatus(DateTime.now(DateTimeZone.UTC))

  /**
   * send the notification if the notification set policy is not specified (meaning
   * send all notifications) or if the policy is specified and this specific notification
   * type is in the notification set.
   */
  def sendNotification(notification: Notification): Unit = notification match {
    case alert: Alert =>
      notificationService ! alert
    case _ =>
      if (policy.notifications.isEmpty)
        notificationService ! notification
      else if (policy.notifications.get.contains(notification.kind))
        notificationService ! notification
  }

  def sendNotifications(notifications: Iterable[Notification]): Unit = {
    notifications.foreach(sendNotification)
  }

  /**
   * send probe status to the state service, and wait for acknowledgement.  if update is
   * acknowledged then send notifications, otherwise log an error.
   */
  def commitStatusAndNotify(status: ProbeStatus, notifications: Vector[Notification]): Future[ProbeStatusCommitted] = {
    stateService.ask(ProbeState(status, probeGeneration)).andThen {
      case Success(committed) =>
        // FIXME: what is the impact if status messages to parent are reordered?
        parent ! status
        self ! SendNotifications(notifications)
      // FIXME: what is the impact on consistency if commit fails?
      case Failure(ex) =>
        log.error(ex, "failed to commit probe state")
    }.mapTo[ProbeStatusCommitted]
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
   *
   */
  def acknowledgeProbe(command: AcknowledgeProbe, caller: ActorRef): Unit = correlationId match {
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
      }.pipeTo(caller)
  }

  /**
   *
   */
  def unacknowledgeProbe(command: UnacknowledgeProbe, caller: ActorRef): Unit = acknowledgementId match {
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

  /**
   *
   */
  def appendComment(command: AppendProbeWorknote, caller: ActorRef): Unit = acknowledgementId match {
    case None =>
      sender() ! ProbeOperationFailed(command, new ApiException(ResourceNotFound))
    case Some(acknowledgement) if acknowledgement != command.acknowledgementId =>
      sender() ! ProbeOperationFailed(command, new ApiException(BadRequest))
    case Some(acknowledgement) =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      trackingService.tell(AppendWorknote(acknowledgement, timestamp, command.comment, command.internal.getOrElse(false)), sender())
  }

  /**
   *
   */
  def squelchProbe(command: SetProbeSquelch, caller: ActorRef): Unit = {
    if (squelch == command.squelch) {
      sender() ! ProbeOperationFailed(command, new ApiException(BadRequest))
    } else {
      val timestamp = DateTime.now(DateTimeZone.UTC)
      squelch = command.squelch
      val status = ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
      val notifications = if (command.squelch) Vector(NotifySquelched(probeRef, timestamp)) else Vector(NotifyUnsquelched(probeRef, timestamp))
      // update state and send notifications
      commitStatusAndNotify(status, notifications).map { _ =>
        SetProbeSquelchResult(command, command.squelch)
      }.recover {
        case ex =>
          // FIXME: what is the impact on consistency if commit fails?
          log.error(ex, "failed to commit probe state")
          ProbeOperationFailed(command, ex)
      }.pipeTo(caller)
    }
  }
}

trait ProbeFSMState
case object InitializingProbeFSMState extends ProbeFSMState
case object RetiringProbeFSMState extends ProbeFSMState
case object RetiredProbeFSMState extends ProbeFSMState

trait ProbeFSMData
case object NoData extends ProbeFSMData

