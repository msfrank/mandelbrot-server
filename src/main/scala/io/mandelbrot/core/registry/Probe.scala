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
import akka.persistence.{Recover, SnapshotOffer, EventsourcedProcessor}
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.duration._
import java.util.UUID

import io.mandelbrot.core.notification._
import io.mandelbrot.core.state.StateService
import io.mandelbrot.core.message.StatusMessage
import io.mandelbrot.core.{ResourceNotFound, Conflict, BadRequest, ApiException}

/**
 *
 */
class Probe(probeRef: ProbeRef, parent: ActorRef, stateService: ActorRef) extends EventsourcedProcessor with ActorLogging {
  import Probe._
  import ProbeSystem.{InitProbe,RetireProbe}
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
  var detail: Option[String] = None
  var lastChange: Option[DateTime] = None
  var lastUpdate: Option[DateTime] = None
  var correlationId: Option[UUID] = None
  var acknowledgementId: Option[UUID] = None
  var squelch: Boolean = false
  var notifier: NotificationPolicy = new EscalatePolicy(parent)
  var timer: Option[Cancellable] = None
  val flapQueue: FlapQueue = new FlapQueue(flapCycles, flapWindow)

  setTimer()

  override def preStart(): Unit = {
    self ! Recover()
  }

  override def postStop(): Unit = {
    for (current <- timer)
      current.cancel()
    timer = None
    //log.debug("snapshotting {}", processorId)
    //saveSnapshot(ProbeSnapshot(lifecycle, health, summary, detail, lastChange, lastUpdate, correlationId, acknowledgementId, squelch))
  }

  def receiveCommand = {

    case InitProbe =>
      persist(ProbeInitializes(DateTime.now(DateTimeZone.UTC)))(updateState(_, recovering = false))

    case ProbeStateTimeout =>
      val correlation = if (health == ProbeHealthy) Some(UUID.randomUUID()) else None
      persist(ProbeExpires(correlation, DateTime.now(DateTimeZone.UTC)))(updateState(_, recovering = false))

    case message: StatusMessage =>
      val correlation = if (health == ProbeHealthy && message.health != ProbeHealthy) Some(UUID.randomUUID()) else None
      persist(ProbeUpdates(message, correlation, DateTime.now(DateTimeZone.UTC)))(updateState(_, recovering = false))

    case query: GetProbeStatus =>
      val status = ProbeStatus(probeRef, DateTime.now(DateTimeZone.UTC), lifecycle, health, summary, detail, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
      sender() ! GetProbeStatusResult(query, status)

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
          persist(UserAcknowledges(acknowledgement, command.message, timestamp))(updateState(_, recovering = false))
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
      detail = snapshot.detail
      lastChange = snapshot.lastChange
      lastUpdate = snapshot.lastUpdate
      correlationId = snapshot.correlationId
      acknowledgementId = snapshot.acknowledgementId
      squelch = snapshot.squelch
  }

  def updateState(event: Event, recovering: Boolean) = event match {

    case ProbeInitializes(timestamp) =>
      lifecycle = ProbeJoining
      summary = None
      detail = None
      lastUpdate = Some(timestamp)
      lastChange = Some(timestamp)
      if (!recovering) {
        // notify state service about updated state
        stateService ! ProbeStatus(probeRef, timestamp, lifecycle, health, summary, detail, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
        // TODO: there should likely be a notification sent here
      }

    case ProbeUpdates(message, correlation, timestamp) =>
      summary = Some(message.summary)
      detail = message.detail
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
        stateService ! ProbeStatus(probeRef, timestamp, lifecycle, health, summary, detail, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
        // send lifecycle notifications
        if (lifecycle != oldLifecycle)
          notifier.notify(NotifyLifecycleChanges(probeRef, message.timestamp, oldLifecycle, lifecycle))
        // send health notifications
        if (flapQueue.isFlapping)
          notifier.notify(NotifyHealthFlaps(probeRef, message.timestamp, correlationId, flapQueue.flapStart))
        else if (oldHealth != health)
          notifier.notify(NotifyHealthChanges(probeRef, message.timestamp, correlationId, oldHealth, health))
        else
          notifier.notify(NotifyHealthUpdates(probeRef, message.timestamp, correlationId, health))
      }
      // reset the timer
      setTimer()

    case ProbeExpires(correlation, timestamp) =>
      val oldHealth = health
      // update health
      health = ProbeUnknown
      summary = None
      detail = None
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
        // notify state service if we transition to unknown
        if (health != oldHealth)
          stateService ! ProbeStatus(probeRef, timestamp, lifecycle, health, summary, detail, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
        // send health notifications
        if (flapQueue.isFlapping)
          notifier.notify(NotifyHealthFlaps(probeRef, timestamp, correlationId, flapQueue.flapStart))
        else if (health != oldHealth)
          notifier.notify(NotifyHealthChanges(probeRef, timestamp, correlationId, oldHealth, health))
        else
          notifier.notify(NotifyHealthExpires(probeRef, timestamp, correlationId))
      }
      // reset the timer
      setTimer()


    case UserAcknowledges(acknowledgement, message, timestamp) =>
      val correlation = correlationId.get
      acknowledgementId = Some(acknowledgement)
      if (!recovering) {
        // notify state service that we are acknowledged
        stateService ! ProbeStatus(probeRef, timestamp, lifecycle, health, summary, detail, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
        // send acknowledgement notification
        notifier.notify(NotifyAcknowledged(probeRef, timestamp, correlation, acknowledgement))
      }

    case UserSetsSquelch(setSquelch, timestamp) =>
      squelch = setSquelch
      if (!recovering) {
        if (setSquelch)
          notifier.notify(NotifySquelched(probeRef, timestamp))
        else
          notifier.notify(NotifyUnsquelched(probeRef, timestamp))
      }

    case ProbeRetires(timestamp) =>
      val oldLifecycle = lifecycle
      lifecycle = ProbeRetired
      summary = None
      detail = None
      lastUpdate = Some(timestamp)
      lastChange = Some(timestamp)
      if (!recovering) {
        // notify state service about updated state
        stateService ! ProbeStatus(probeRef, timestamp, lifecycle, health, summary, detail, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
        // send lifecycle notifications
        notifier.notify(NotifyLifecycleChanges(probeRef, timestamp, oldLifecycle, lifecycle))
      }
      // FIXME: stop timer?
  }

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
  def props(probeRef: ProbeRef, parent: ActorRef, stateService: ActorRef) = Props(classOf[Probe], probeRef, parent, stateService)

  sealed trait Event
  case class ProbeInitializes(timestamp: DateTime) extends Event
  case class ProbeUpdates(state: StatusMessage, correlationId: Option[UUID], timestamp: DateTime) extends Event
  case class ProbeExpires(correlationId: Option[UUID], timestamp: DateTime) extends Event
  case class UserAcknowledges(acknowledgementId: UUID, message: String, timestamp: DateTime) extends Event
  case class UserSetsSquelch(squelch: Boolean, timestamp: DateTime) extends Event
  case class ProbeRetires(timestamp: DateTime) extends Event
  case object ProbeStateTimeout

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
case object ProbeStatic extends ProbeLifecycle  { override def toString = "static" }

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
                       detail: Option[String],
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

case class AcknowledgeProbe(probeRef: ProbeRef, correlationId: UUID, message: String) extends ProbeCommand
case class AcknowledgeProbeResult(op: AcknowledgeProbe, acknowledgementId: UUID)
