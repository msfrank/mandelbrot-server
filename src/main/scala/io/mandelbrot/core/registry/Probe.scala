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
  var notifier: NotificationPolicy = new EmitPolicy(context.system)
  var squelch: Boolean = false
  var timer: Option[Cancellable] = None
  val flapQueue: FlapQueue = new FlapQueue(flapCycles, flapWindow)

  /* */
  val stateService = StateService(context.system)
  stateService ! UpdateProbeStatus(probeRef, DateTime.now(DateTimeZone.UTC), lifecycle, health, None, None)

  setTimer()

  def receiveCommand = {

    case ProbeStateTimeout =>
      persist(ProbeExpires(DateTime.now(DateTimeZone.UTC)))(updateState(_, recovering = false))

    case message: StatusMessage =>
      persist(ProbeUpdates(message, DateTime.now(DateTimeZone.UTC)))(updateState(_, recovering = false))

    case GetProbeState =>
      sender() ! ProbeState(lifecycle, health, summary, lastUpdate, lastChange, squelch)

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

    case ProbeUpdates(message, timestamp) =>
      summary = Some(message.summary)
      lastUpdate = Some(timestamp)
      val oldHealth = health
      val oldLifecycle = lifecycle
      // update health
      health = message.health
      if (health != oldHealth) {
        lastChange = Some(timestamp)
        flapQueue.push(message.timestamp)
      }
      // update lifecycle
      if (oldLifecycle == ProbeJoining)
        lifecycle = ProbeKnown
      if (!recovering) {
        // send health notifications
        if (flapQueue.isFlapping)
          notifier.notify(NotifyHealthFlaps(probeRef, flapQueue.flapStart, message.timestamp))
        else if (oldHealth != health)
          notifier.notify(NotifyHealthChanges(probeRef, oldHealth, health, message.timestamp))
        else
          notifier.notify(NotifyHealthUpdates(probeRef, health, message.timestamp))
        // send lifecycle notifications
        if (lifecycle != oldLifecycle)
          notifier.notify(NotifyLifecycleChanges(probeRef, oldLifecycle, lifecycle, message.timestamp))
        //
        stateService ! UpdateProbeStatus(probeRef, timestamp, lifecycle, health, Some(message.summary), message.detail)
      }
      // reset the timer
      setTimer()

    case ProbeExpires(timestamp) =>
      val oldHealth = health
      // update health
      health = ProbeUnknown
      if (health != oldHealth) {
        lastChange = Some(timestamp)
        flapQueue.push(timestamp)
      }
      if (!recovering) {
        // send health notifications
        if (flapQueue.isFlapping)
          notifier.notify(NotifyHealthFlaps(probeRef, flapQueue.flapStart, timestamp))
        else if (oldHealth != health)
          notifier.notify(NotifyHealthChanges(probeRef, oldHealth, health, timestamp))
        else
          notifier.notify(NotifyHealthExpires(probeRef, timestamp))
      }
      // reset the timer
      setTimer()
  }

  def setTimer(duration: Option[FiniteDuration] = None): Unit = {
    for (current <- timer)
      current.cancel()
    duration match {
      case Some(delay) =>
        context.system.scheduler.scheduleOnce(delay, self, ProbeStateTimeout)
      case None =>
        lifecycle match {
          case ProbeJoining =>
            context.system.scheduler.scheduleOnce(joiningTimeout, self, ProbeStateTimeout)
          case ProbeLeaving =>
            context.system.scheduler.scheduleOnce(leavingTimeout, self, ProbeStateTimeout)
          case _ =>
            context.system.scheduler.scheduleOnce(runningTimeout, self, ProbeStateTimeout)
        }
    }
  }
}

object Probe {
  def props(probeRef: ProbeRef, parent: ActorRef) = Props(classOf[Probe], probeRef, parent)
  sealed trait Event
  case class ProbeUpdates(state: StatusMessage, timestamp: DateTime) extends Event
  case class ProbeExpires(timestamp: DateTime) extends Event
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


case object GetProbeState
case class ProbeState(lifecycle: ProbeLifecycle, health: ProbeHealth, summary: Option[String], lastUpdate: Option[DateTime], lastChange: Option[DateTime], squelched: Boolean)
