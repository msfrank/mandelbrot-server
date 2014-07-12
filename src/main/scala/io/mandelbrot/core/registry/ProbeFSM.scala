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
import io.mandelbrot.core.{ResourceNotFound, ApiException}
import io.mandelbrot.core.registry.Probe.SendNotifications
import org.joda.time.DateTime
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

import io.mandelbrot.core.notification._
import io.mandelbrot.core.state.{ProbeStatusCommitted, ProbeState}

/**
 *
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
      // otherwise replay any stashed messages and transition to running
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
   *
   */
  def applyBehaviorPolicy(behavior: BehaviorPolicy) = policy.behavior match {
    case behavior: AggregateBehaviorPolicy =>
      log.debug("probe {} becomes aggregate", probeRef)
      goto(AggregateProbeFSMState) using AggregateProbeFSMState(behavior)
    case behavior: ScalarBehaviorPolicy =>
      log.debug("probe {} becomes scalar", probeRef)
      goto(ScalarProbeFSMState) using ScalarProbeFSMState(behavior)
  }

  /**
   * send the notification if the notification set policy is not specified (meaning
   * send all notifications) or if the policy is specified and this specific notification
   * type is in the notification set.
   */
  def sendNotification(notification: Notification): Unit = notification match {
    case alert: Alert =>
      parent ! alert
    case _ =>
      if (policy.notifications.isEmpty)
        notificationService ! notification
      else if (policy.notifications.get.contains(notification.kind))
        notificationService ! notification
  }

  /**
   * send probe status to the state service, and wait for acknowledgement.  if update is
   * acknowledged then send notifications, otherwise log an error.
   */
  def commitStatusAndNotify(status: ProbeStatus, notifications: Vector[Notification]): Future[ProbeStatusCommitted] = {
    stateService.ask(ProbeState(status, probeGeneration)).andThen {
      case Success(committed) =>
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

}

trait ProbeFSMState
case object InitializingProbeFSMState extends ProbeFSMState
case object RetiredProbeFSMState extends ProbeFSMState

trait ProbeFSMData
case object NoData extends ProbeFSMData

