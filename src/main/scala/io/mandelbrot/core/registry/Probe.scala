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

import akka.actor.{Cancellable, ActorLogging, ActorRef, Props}
import akka.persistence.{SnapshotOffer, EventsourcedProcessor}
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.duration._

import io.mandelbrot.core.notification._

import io.mandelbrot.core.state.{UpdateProbeStatus, StateService}
import io.mandelbrot.core.messagestream.StatusMessage
import java.util.UUID
import io.mandelbrot.core.{ResourceNotFound, Conflict, BadRequest, ApiException}

/**
 *
 */
class Probe(probeRef: ProbeRef, parent: ActorRef) extends EventsourcedProcessor with ActorLogging {
  import Probe._
  import context.dispatcher

  // config
  override def processorId = probeRef.toString
  val flapCycles = 10
  val flapWindow = 5.minutes
  val joiningTimeout = 5.minutes
  val runningTimeout = 1.minute
  val leavingTimeout = 5.minutes

  // state
  var lifecycle: ProbeLifecycle = ProbeJoining
  var health: ProbeHealth = ProbeUnknown
  var summary: Option[String] = None
  var lastChange: Option[DateTime] = None
  var lastUpdate: Option[DateTime] = None
  var correlationId: Option[UUID] = None
  var acknowledgementId: Option[UUID] = None
  var squelch: Boolean = false
  var notifier: NotificationPolicy = new EmitPolicy(context.system)
  var timer: Option[Cancellable] = None
  val flapQueue: FlapQueue = new FlapQueue(flapCycles, flapWindow)

  /* */
  val stateService = StateService(context.system)
  stateService ! UpdateProbeStatus(probeRef, DateTime.now(DateTimeZone.UTC), lifecycle, health, None, None)

  setTimer()

  def receiveCommand = {

    case ProbeStateTimeout =>
      val correlation = if (health == ProbeHealthy) Some(UUID.randomUUID()) else None
      persist(ProbeExpires(correlation, DateTime.now(DateTimeZone.UTC)))(updateState(_, recovering = false))

    case message: StatusMessage =>
      val correlation = if (health == ProbeHealthy && message.health != ProbeHealthy) Some(UUID.randomUUID()) else None
      persist(ProbeUpdates(message, correlation, DateTime.now(DateTimeZone.UTC)))(updateState(_, recovering = false))

    case query: GetProbeState =>
      val state = ProbeState(probeRef, lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
      sender() ! GetProbeStateResult(query, state)

    case command: AcknowledgeProbe =>
      val result = correlationId match {
        case None =>
          ProbeOperationFailed(command, new ApiException(ResourceNotFound))
        case Some(correlation) if acknowledgementId.isDefined =>
          ProbeOperationFailed(command, new ApiException(Conflict))
        case Some(correlation) if correlation != command.correlationId =>
          log.debug("failed to acknowledge")
          ProbeOperationFailed(command, new ApiException(BadRequest))
        case Some(correlation) =>
          val acknowledgement = UUID.randomUUID()
          val timestamp = DateTime.now(DateTimeZone.UTC)
          persist(UserAcknowledges(acknowledgement, timestamp))(updateState(_, recovering = false))
          AcknowledgeProbeResult(command, acknowledgement)
      }
      sender() ! result

    case command: SetProbeSquelch =>
      val result = if (squelch != command.squelch) {
        persist(UserSetsSquelch(command.squelch, DateTime.now(DateTimeZone.UTC)))(updateState(_, recovering = false))
        SetProbeSquelchResult(command, command.squelch)
      } else ProbeOperationFailed(command, new ApiException(BadRequest))
      sender() ! result

    case notification: Notification =>
      notifier.notify(notification)
  }

  def receiveRecover = {

    case event: Event =>
      updateState(event, recovering = true)

    case offer: SnapshotOffer =>
      log.debug("received snapshot offer: metadata={}, snapshot={}", offer.metadata, offer.snapshot)
  }

  def updateState(event: Event, recovering: Boolean) = event match {

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
        flapQueue.push(message.timestamp)
        // we transition from healthy to non-healthy
        if (oldHealth == ProbeHealthy) {
          correlationId = correlation
          acknowledgementId = None
        }
        // we transition from non-healthy to healthy
        else if (health == ProbeHealthy) {
          correlationId = None
          acknowledgementId = None
        }
      }
      if (!recovering) {
        // notify state service about updated state
        stateService ! UpdateProbeStatus(probeRef, timestamp, lifecycle, health, Some(message.summary), message.detail)
        // send health notifications if not squelched
        if (!squelch) {
          // send lifecycle notifications
          if (lifecycle != oldLifecycle)
            notifier.notify(NotifyLifecycleChanges(probeRef, oldLifecycle, lifecycle, message.timestamp))
          if (flapQueue.isFlapping)
            notifier.notify(NotifyHealthFlaps(probeRef, flapQueue.flapStart, message.timestamp))
          else if (oldHealth != health)
            notifier.notify(NotifyHealthChanges(probeRef, oldHealth, health, message.timestamp))
          else
            notifier.notify(NotifyHealthUpdates(probeRef, health, message.timestamp))
        }
      }
      // reset the timer
      setTimer()

    case ProbeExpires(correlation, timestamp) =>
      val oldHealth = health
      // update health
      health = ProbeUnknown
      if (health != oldHealth) {
        lastChange = Some(timestamp)
        flapQueue.push(timestamp)
        // we transition from healthy to non-healthy
        if (oldHealth == ProbeHealthy) {
          correlationId = correlation
          acknowledgementId = None
        }
      }
      if (!recovering) {
        // notify state service about updated state
        stateService ! UpdateProbeStatus(probeRef, timestamp, lifecycle, health, None, None)
        // send health notifications if not squelched
        if (!squelch) {
          if (flapQueue.isFlapping)
            notifier.notify(NotifyHealthFlaps(probeRef, flapQueue.flapStart, timestamp))
          else if (oldHealth != health)
            notifier.notify(NotifyHealthChanges(probeRef, oldHealth, health, timestamp))
          else
            notifier.notify(NotifyHealthExpires(probeRef, timestamp))
        }
      }
      // reset the timer
      setTimer()


    case UserAcknowledges(acknowledgement, timestamp) =>
      acknowledgementId = Some(acknowledgement)
      if (!recovering) {
        notifier.notify(NotifyAcknowledged(probeRef, correlationId.get, acknowledgement, timestamp))
      }

    case UserSetsSquelch(setSquelch, timestamp) =>
      squelch = setSquelch
      if (!recovering) {
        if (setSquelch)
          notifier.notify(NotifySquelched(probeRef, timestamp))
        else
          notifier.notify(NotifyUnsquelched(probeRef, timestamp))
      }
  }

  def notify(notification: Notification): Unit = if (!squelch) notifier.notify(notification)

  def setTimer(duration: Option[FiniteDuration] = None): Unit = {
    for (current <- timer)
      current.cancel()
    timer = duration match {
      case Some(delay) =>
        Some(context.system.scheduler.scheduleOnce(delay, self, ProbeStateTimeout))
      case None =>
        lifecycle match {
          case ProbeJoining =>
            Some(context.system.scheduler.scheduleOnce(joiningTimeout, self, ProbeStateTimeout))
          case ProbeLeaving =>
            Some(context.system.scheduler.scheduleOnce(leavingTimeout, self, ProbeStateTimeout))
          case _ =>
            Some(context.system.scheduler.scheduleOnce(runningTimeout, self, ProbeStateTimeout))
        }
    }
  }
}

object Probe {
  def props(probeRef: ProbeRef, parent: ActorRef) = Props(classOf[Probe], probeRef, parent)
  sealed trait Event
  case class ProbeUpdates(state: StatusMessage, correlationId: Option[UUID], timestamp: DateTime) extends Event
  case class ProbeExpires(correlationId: Option[UUID], timestamp: DateTime) extends Event
  case class UserAcknowledges(acknowledgementId: UUID, timestamp: DateTime) extends Event
  case class UserSetsSquelch(squelch: Boolean, timestamp: DateTime) extends Event
  case object ProbeStateTimeout
}

/* object lifecycle */
sealed abstract class ProbeLifecycle(val value: String) {
  override def toString = value
}
case object ProbeJoining extends ProbeLifecycle("joining")
case object ProbeKnown extends ProbeLifecycle("known")
case object ProbeLeaving extends ProbeLifecycle("leaving")
case object ProbeRetired extends ProbeLifecycle("retired")

/* object state */
sealed abstract class ProbeHealth(val value: String) {
  override def toString = value
}
case object ProbeHealthy extends ProbeHealth("healthy")
case object ProbeDegraded extends ProbeHealth("degraded")
case object ProbeFailed extends ProbeHealth("failed")
case object ProbeUnknown extends ProbeHealth("unknown")

/* */
case class ProbeState(probeRef: ProbeRef,
                      lifecycle: ProbeLifecycle,
                      health: ProbeHealth,
                      summary: Option[String],
                      lastUpdate: Option[DateTime],
                      lastChange: Option[DateTime],
                      correlation: Option[UUID],
                      acknowledged: Option[UUID],
                      squelched: Boolean)

/* */
sealed trait ProbeOperation { val probeRef: ProbeRef }
sealed trait ProbeCommand extends ProbeOperation
sealed trait ProbeQuery extends ProbeOperation
case class ProbeOperationFailed(op: ProbeOperation, failure: Throwable)

case class GetProbeState(probeRef: ProbeRef) extends ProbeQuery
case class GetProbeStateResult(op: GetProbeState, state: ProbeState)

case class SetProbeSquelch(probeRef: ProbeRef, squelch: Boolean) extends ProbeCommand
case class SetProbeSquelchResult(op: SetProbeSquelch, squelch: Boolean)

case class AcknowledgeProbe(probeRef: ProbeRef, correlationId: UUID) extends ProbeCommand
case class AcknowledgeProbeResult(op: AcknowledgeProbe, acknowledgementId: UUID)
