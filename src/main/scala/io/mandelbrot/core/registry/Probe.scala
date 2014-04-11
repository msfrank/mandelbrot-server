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

import akka.actor.{ActorRef, LoggingFSM, Props}
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.duration._

import io.mandelbrot.core.notification._

import Probe.{State,Data}

/**
 *
 */
class Probe(probeRef: ProbeRef, parent: ActorRef, notificationManager: ActorRef) extends LoggingFSM[State,Data] {
  import Probe._

  // config
  val flapCycles = 10
  val flapWindow = 5.minutes
  val initializationTimeout = 5.minutes
  val objectStateTimeout = 1.minute

  // state
  var lifecycle: ProbeLifecycle = ProbeJoining
  var state: ProbeState = ProbeUnknown
  var notifier: NotificationPolicy = new NotifyParentPolicy()
  val flapQueue: FlapQueue = new FlapQueue(flapCycles, flapWindow)

  startWith(Initializing, NoData)
  setTimer("objectState", ProbeStateTimeout, initializationTimeout)

  when(Initializing) {

    case Event(UpdateState(newState, changeTime), NoData) =>
      state = newState
      lifecycle = ProbeKnown
      flapQueue.push(changeTime)
      setTimer("objectState", ProbeStateTimeout, objectStateTimeout)
      goto(Running) using Running(changeTime, changeTime)

    case Event(ProbeStateTimeout, NoData) =>
      setTimer("objectState", ProbeStateTimeout, objectStateTimeout)
      val changeTime = DateTime.now(DateTimeZone.UTC)
      goto(Running) using Running(changeTime, changeTime)

    case Event(notification: Notification, NoData) =>
      notifier.notify(notification)
      stay()
  }

  when(Running) {

    case Event(UpdateState(currentState, updateTime), Running(lastUpdate, lastChange)) =>
      setTimer("objectState", ProbeStateTimeout, objectStateTimeout)
      /* object state has changed */
      if (currentState != state) {
        val oldState = state
        state = currentState
        flapQueue.push(updateTime)
        if (flapQueue.isFlapping) {
          notifier.notify(NotifyStateFlaps(probeRef, updateTime))
          goto(Flapping) using Flapping(updateTime, updateTime, updateTime)
        }
        else {
          notifier.notify(NotifyStateChanges(probeRef, oldState, currentState))
          stay() using Running(updateTime, updateTime)
        }
      }
      /* object state remains the same */
      else {
        notifier.notify(NotifyStateUpdates(probeRef, currentState, updateTime))
        stay() using Running(updateTime, lastChange)
      }

    case Event(ProbeStateTimeout, Running(lastUpdate, lastChange)) =>
      stay()
  }

  when(Flapping) {
    case Event(_, _) =>
      stay()
  }

  initialize()
}

object Probe {
  def props(probeRef: ProbeRef, parent: ActorRef, notificationManager: ActorRef) = {
    Props(classOf[Probe], probeRef, parent, notificationManager)
  }

  sealed trait State
  case object Initializing extends State
  case object Running extends State
  case object Flapping extends State

  sealed trait Data
  case class Running(lastUpdate: DateTime, lastChange: DateTime) extends Data
  case class Flapping(flapStart: DateTime, lastUpdate: DateTime, lastChange: DateTime) extends Data
  case object NoData extends Data

  case object ProbeStateTimeout
}

/* object lifecycle */
sealed trait ProbeLifecycle
case object ProbeJoining extends ProbeLifecycle
case object ProbeKnown extends ProbeLifecycle
case object ProbeLeaving extends ProbeLifecycle
case object ProbeRetired extends ProbeLifecycle
case object ProbeInMaintenance extends ProbeLifecycle

/* object state */
sealed trait ProbeState
case object ProbeHealthy extends ProbeState
case object ProbeDegraded extends ProbeState
case object ProbeFailed extends ProbeState
case object ProbeUnknown extends ProbeState

case class UpdateState(state: ProbeState, changeTime: DateTime)

case class UpdateLifecycle(lifecycle: ProbeLifecycle, changeTime: DateTime)