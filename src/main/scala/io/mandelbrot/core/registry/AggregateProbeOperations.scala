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

import akka.actor.Actor

import scala.collection.mutable

/**
 *
 */
trait AggregateProbeOperations extends ProbeFSM with Actor {

  // for ask pattern
  import context.dispatcher
  implicit val timeout: akka.util.Timeout

  when(AggregateProbeFSMState) {

    /*
     * if the set of direct children has changed, and we are rolling up alerts, then
     * update our escalation state.  if the probe policy has changed, then update any
     * running timers.
     */
    case Event(UpdateProbe(directChildren, newPolicy, lsn), state: AggregateProbeFSMState) =>
      (children -- directChildren).foreach { ref => state.children.remove(ref)}
      (directChildren -- children).foreach { ref => state.children.put(ref, None)}
      children = directChildren
      policy = newPolicy
      resetExpiryTimer()
      // FIXME: reset alert timer as well?
      stay()
  }
}

case class AggregateProbeFSMState(behavior: AggregateBehaviorPolicy,
                                  children: mutable.HashMap[ProbeRef,Option[ProbeStatus]],
                                  flapQueue: Option[FlapQueue]) extends ProbeFSMData

case object AggregateProbeFSMState extends ProbeFSMState {
  def apply(behavior: AggregateBehaviorPolicy): AggregateProbeFSMState = {
    AggregateProbeFSMState(behavior, new mutable.HashMap[ProbeRef,Option[ProbeStatus]], None)
  }
}
