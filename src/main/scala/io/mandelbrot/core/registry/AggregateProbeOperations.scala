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

case object AggregateProbeFSMState extends ProbeFSMState
case class AggregateProbeFSMState(children: mutable.HashMap[ProbeRef,Option[ProbeStatus]]) extends ProbeFSMData
