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

package io.mandelbrot.core.entity

import akka.actor._
import akka.cluster.{MemberStatus, Member}
import akka.contrib.pattern.ClusterSingletonManager
import scala.collection.SortedSet
import scala.concurrent.duration._

/**
 *
 */
class ClusterBalancer(settings: ClusterSettings, services: ActorRef, nodePath: Iterable[String])
  extends LoggingFSM[ClusterBalancer.State,ClusterBalancer.Data] {
  import ClusterBalancer._
  import context.dispatcher

  // config
  val balancerInterval = 1.minute

  // state
  var members: SortedSet[Member] = SortedSet.empty

  // subscribe to events from ClusterMonitor
  context.system.eventStream.subscribe(self, classOf[ClusterState])

  startWith(Down, Down(1))

  when(Down) {

    // cluster is up, start the balancer
    case Event(event: ClusterUp, state: Down) =>
      members = event.members
      val nodes: Map[Address,ActorPath] = members.filter { member =>
        member.status.equals(MemberStatus.Up)
      }.map { member =>
        member.address -> (RootActorPath(member.address) / nodePath)
      }.toMap
      val props = BalancerTask.props(services, self, nodes, settings.totalShards)
      val balancer = context.actorOf(props, "balancer-%d".format(state.version))
      goto(Running) using Running(balancer, state.version + 1)

    // do nothing
    case Event(event: ClusterState, state: Down) =>
      members = event.members
      stay()
  }

  when(Waiting) {

    // delay has completed, start the balancer
    case Event(StartBalancing, state: Waiting) =>
      val nodes: Map[Address,ActorPath] = members.filter { member =>
        member.status.equals(MemberStatus.Up)
      }.map { member =>
        member.address -> (RootActorPath(member.address) / nodePath)
      }.toMap
      val props = BalancerTask.props(services, self, nodes, settings.totalShards)
      val balancer = context.actorOf(props, "balancer-%d".format(state.version))
      context.watch(balancer)
      goto(Running) using Running(balancer, state.version + 1)

    // cluster is down, cancel the delay
    case Event(event: ClusterDown, state: Waiting) =>
      members = event.members
      state.delay.cancel()
      goto(Down) using Down(state.version)

    // do nothing
    case Event(event: ClusterState, state: Waiting) =>
      members = event.members
      stay()
  }

  when(Running) {

    // balancer has completed, but terminate the balancer actor just to be safe
    case Event(event: BalancerComplete, state: Running) =>
      context.stop(state.balancer)
      stay() using Stopping(state.version)

    // cluster is down, terminate the balancer actor
    case Event(event: ClusterDown, state: Running) =>
      members = event.members
      context.stop(state.balancer)
      stay() using Down(state.version)

    // do nothing
    case Event(event: ClusterState, state: Running) =>
      members = event.members
      stay()

    // balancer completed and actor has terminated
    case Event(Terminated(ref), state: Stopping) =>
      val delay = context.system.scheduler.scheduleOnce(balancerInterval, self, StartBalancing)
      goto(Waiting) using Waiting(delay, state.version)

    // balancer has terminated forcibly after cluster down
    case Event(Terminated(ref), state: Down) =>
      goto(Down) using state

    // balancer has terminated unexpectedly
    case Event(Terminated(ref), state: Running) =>
      val delay = context.system.scheduler.scheduleOnce(balancerInterval, self, StartBalancing)
      goto(Waiting) using Waiting(delay, state.version)
  }

  initialize()
}

object ClusterBalancer {
  def props(settings: ClusterSettings, services: ActorRef, nodePath: Iterable[String]) = {
    val props = Props(classOf[ClusterBalancer], settings, services, nodePath)
    val role = settings.clusterRole
    val maxHandOverRetries = settings.maxHandOverRetries
    val maxTakeOverRetries = settings.maxTakeOverRetries
    val retryInterval = settings.retryInterval
    ClusterSingletonManager.props(props, "singleton", PoisonPill, role, maxHandOverRetries, maxTakeOverRetries, retryInterval)
  }

  case object StartBalancing

  sealed trait State
  case object Waiting extends State
  case object Running extends State
  case object Down extends State

  sealed trait Data
  case class Down(version: Long) extends Data
  case class Waiting(delay: Cancellable, version: Long) extends Data
  case class Running(balancer: ActorRef, version: Long) extends Data
  case class Stopping(version: Long) extends Data
}
