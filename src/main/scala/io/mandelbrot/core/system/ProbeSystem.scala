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

package io.mandelbrot.core.system

import akka.actor._
import io.mandelbrot.core.entity.Entity
import scala.collection.mutable
import java.net.URI

import io.mandelbrot.core._
import io.mandelbrot.core.model._
import io.mandelbrot.core.registry._
import io.mandelbrot.core.metrics.MetricsBus

/**
 * the ProbeSystem manages a collection of Probes underneath a URI.  the ProbeSystem
 * is responsible for adding and removing probes when the registration changes, as well
 * as updating probes when policy changes.  lastly, the ProbeSystem acts as an endpoint
 * for commands and queries operating on sets of probes in the system.
 */
class ProbeSystem(services: ActorRef) extends LoggingFSM[ProbeSystem.State,ProbeSystem.Data] with Stash {
  import ProbeSystem._

  // config
  val settings = ServerConfig(context.system).settings

  // state
  var probes: Map[ProbeRef,ProbeActor] = Map.empty
  val retiredProbes = new mutable.HashMap[ActorRef,(ProbeRef,Long)]
  val zombieProbes = new mutable.HashSet[ProbeRef]
  val metricsBus = new MetricsBus()

  override def preStart(): Unit = {
    startWith(SystemIncubating, SystemWaiting)
    initialize()
  }

  when(SystemIncubating) {

    case Event(op: RegisterProbeSystem, _) =>
      services ! CreateProbeSystemEntry(op.uri, op.registration)
      goto(SystemRegistering) using SystemRegistering(op, sender())

    case Event(entity: Entity, _) =>
      val uri = new URI(entity.entityKey)
      services ! GetProbeSystemEntry(uri)
      goto(SystemInitializing) using SystemInitializing(uri)
      
    case _: Event => stop()
  }

  when(SystemRegistering) {

    case Event(result: CreateProbeSystemEntryResult, state: SystemRegistering) =>
      state.sender ! RegisterProbeSystemResult(state.op, result.lsn)
      goto(SystemRunning) using SystemRunning(state.op.uri, result.op.registration, 0)

    case Event(failure: RegistryServiceOperationFailed, state: SystemRegistering) =>
      state.sender ! failure
      goto(SystemFailed) using SystemError(failure.failure)

    case Event(op: ProbeSystemOperation, _) =>
      stash()
      stay()

    case Event(op: ProbeOperation, _) =>
      stash()
      stay()
  }

  when(SystemInitializing) {

    case Event(result: GetProbeSystemEntryResult, state: SystemInitializing) =>
      goto(SystemRunning) using SystemRunning(state.uri, result.registration, 0)

    case Event(failure: RegistryServiceOperationFailed, state: SystemInitializing) =>
      goto(SystemFailed) using SystemError(failure.failure)

    case Event(op: ProbeSystemOperation, _) =>
      stash()
      stay()

    case Event(op: ProbeOperation, _) =>
      stash()
      stay()
  }

  onTransition {
    case _ -> SystemRunning => nextStateData match {
      case state: SystemRunning =>
        log.debug("configuring probe system {}", state.uri)
        unstashAll()
        applyProbeRegistration(state.uri, state.registration, state.lsn)
      case _ =>
    }
  }

  when (SystemRunning) {

    /* get the ProbeSystem lsn */
    case Event(op: ReviveProbeSystem, state: SystemRunning) =>
      stay() replying ReviveProbeSystemResult(op, state.lsn)

    /* get the ProbeSystem spec */
    case Event(query: DescribeProbeSystem, state: SystemRunning) =>
      stay() replying DescribeProbeSystemResult(query, state.registration, state.lsn)

    case Event(query: MatchProbeSystem, state: SystemRunning) =>
      if (query.matchers.isEmpty) stay() replying MatchProbeSystemResult(query, probes.keySet) else {
        val matchingRefs = probes.keys.flatMap { case ref =>
          query.matchers.collectFirst { case matcher if matcher.matches(ref) => ref }
        }.toSet
        stay() replying MatchProbeSystemResult(query, matchingRefs)
      }

    case Event(op: RegisterProbeSystem, state: SystemRunning) =>
      stay() replying ProbeSystemOperationFailed(op, ApiException(Conflict))

    /* update probes */
    case Event(op: UpdateProbeSystem, state: SystemRunning) =>
      goto(SystemUpdating) using SystemUpdating(op, sender(), state)

    /* retire all running probes */
    case Event(op: RetireProbeSystem, state: SystemRunning) =>
      goto(SystemRetiring) using SystemRetiring(op, sender(), state)

    /* ignore probe status from top level probes */
    case Event(status: ProbeStatus, state: SystemRunning) =>
      stay()

    /* forward probe operations to the specified probe */
    case Event(op: ProbeOperation, state: SystemRunning) =>
      probes.get(op.probeRef) match {
        case Some(probeActor: ProbeActor) =>
          probeActor.actor.forward(op)
        case None =>
          sender() ! ProbeOperationFailed(op, ApiException(ResourceNotFound))
      }
      stay()

    /* handle notifications which have been passed up from Probe */
    case Event(notification: NotificationEvent, state: SystemRunning) =>
      services ! notification
      stay()

    /* clean up retired probes, reanimate zombie probes */
    case Event(Terminated(actorRef), state: SystemRunning) =>
      val (probeRef,lsn) = retiredProbes(actorRef)
      probes = probes - probeRef
      retiredProbes.remove(actorRef)
      if (zombieProbes.contains(probeRef)) {
        zombieProbes.remove(probeRef)
        applyProbeRegistration(state.uri, state.registration, state.lsn)
      } else
        log.debug("probe {} has been terminated", probeRef)
      stay()
  }

  onTransition {
    case _ -> SystemUpdating => nextStateData match  {
      case state: SystemUpdating => services ! UpdateProbeSystemEntry(state.op.uri, state.op.registration, state.prev.lsn)
      case _ =>
    }
  }

  when(SystemUpdating) {
    case Event(result: UpdateProbeSystemEntryResult, state: SystemUpdating) =>
      state.sender ! UpdateProbeSystemResult(state.op, result.lsn)
      goto(SystemRunning) using SystemRunning(state.op.uri, state.op.registration, result.lsn)
    case Event(failure: RegistryServiceOperationFailed, state: SystemUpdating) =>
      state.sender ! failure
      goto(SystemRunning) using state.prev
    case Event(_, state: SystemUpdating) =>
      stash()
      stay()
  }

  onTransition {
    case _ -> SystemRetiring => nextStateData match {
      case state: SystemRetiring => services ! DeleteProbeSystemEntry(state.op.uri, state.prev.lsn)
      case _ =>
    }
  }

  when(SystemRetiring) {
    case Event(result: DeleteProbeSystemEntryResult, state: SystemRetiring) =>
      state.sender ! RetireProbeSystemResult(state.op, result.lsn)
      goto(SystemRetired) using SystemRetired(state.prev.uri, state.prev.registration, result.lsn)
    case Event(failure: RegistryServiceOperationFailed, state: SystemRetiring) =>
      state.sender ! failure
      goto(SystemRunning) using state.prev
    case Event(_, state: SystemRetiring) =>
      stash()
      stay()
  }

  onTransition {
    case _ -> SystemRetired => nextStateData match {
      case state: SystemRetired =>
        unstashAll()
        probes.foreach {
          case (probeRef,probeActor) if !retiredProbes.contains(probeActor.actor) =>
            probeActor.actor ! RetireProbe(state.lsn)
            retiredProbes.put(probeActor.actor, (probeRef,state.lsn))
          case _ => // do nothing
        }
      case _ =>
      }
  }

  when(SystemRetired) {
    /* clean up retired probes, reanimate zombie probes */
    case Event(Terminated(actorRef), state: SystemRetired) =>
      val (probeRef,lsn) = retiredProbes(actorRef)
      probes = probes - probeRef
      retiredProbes.remove(actorRef)
      if (zombieProbes.contains(probeRef))
        zombieProbes.remove(probeRef)
      if (probes.nonEmpty) stay() else stop()
    /* system doesn't exist anymore, so return resource not found */
    case Event(op: ProbeSystemOperation, _) =>
      stay() replying ProbeSystemOperationFailed(op, ApiException(ResourceNotFound))
    case Event(op: ProbeOperation, _) =>
      stay() replying ProbeOperationFailed(op, ApiException(ResourceNotFound))
  }

  onTransition {
    case _ -> SystemFailed => unstashAll()
  }

  when(SystemFailed) {
    case Event(op: ProbeSystemOperation, state: SystemError) =>
      stop() replying ProbeSystemOperationFailed(op, state.ex)
    case Event(op: ProbeOperation, state: SystemError) =>
      stop() replying ProbeOperationFailed(op, state.ex)
    /* ignore any other messages */
    case _: Event => stay()
  }

  /**
   * flatten ProbeRegistration into a Set of ProbeRefs
   */
  def spec2RefSet(uri: URI, path: Vector[String], spec: ProbeSpec): Set[ProbeRef] = {
    val iterChildren = spec.children.toSet
    val childRefs = iterChildren.map { case (name: String, childSpec: ProbeSpec) =>
      spec2RefSet(uri, path :+ name, childSpec)
    }.flatten
    childRefs + ProbeRef(uri, path)
  }
  def registration2RefSet(uri: URI, registration: ProbeRegistration): Set[ProbeRef] = {
    registration.probes.flatMap { case (name,spec) =>
      spec2RefSet(uri, Vector(name), spec)
    }.toSet
  }

  /**
   * find the ProbeSpec referenced by path.  NOTE: It is assumed that the specified
   * ProbeRef exists!  if it doesn't, this code will throw an exception.
   */
  def findProbeSpec(spec: ProbeSpec, path: Vector[String]): ProbeSpec = {
    if (path.isEmpty) spec else findProbeSpec(spec.children(path.head), path.tail)
  }
  def findProbeSpec(registration: ProbeRegistration, path: Vector[String]): ProbeSpec = {
    findProbeSpec(registration.probes(path.head), path.tail)
  }

  /**
   * apply the spec to the probe system, adding and removing probes as necessary
   */
  def applyProbeRegistration(uri: URI, registration: ProbeRegistration, lsn: Long): Unit = {
    val specSet = registration2RefSet(uri, registration)
    val probeSet = probes.keySet
    // add new probes
    val probesAdded = specSet -- probeSet
    probesAdded.toVector.sorted.foreach { case ref: ProbeRef =>
      val probeSpec = findProbeSpec(registration, ref.path)
      val directChildren = specSet.filter { _.parentOption match {
        case Some(parent) => parent == ref
        case None => false
      }}
      val probeType = probeSpec.probeType
      val factory = ProbeBehavior.extensions(probeSpec.probeType).configure(probeSpec.properties)
      val actor = ref.parentOption match {
        case Some(parent) if parent.path.nonEmpty =>
          context.actorOf(Probe.props(ref, probes(parent).actor, probeType, directChildren, probeSpec.policy, factory, lsn, services, metricsBus))
        case _ =>
          context.actorOf(Probe.props(ref, self, probeType, directChildren, probeSpec.policy, factory, lsn, services, metricsBus))
      }
      context.watch(actor)
      log.debug("probe {} joins", ref)
      probes = probes + (ref -> ProbeActor(probeSpec, actor))
    }
    // remove stale probes
    val probesRemoved = probeSet -- specSet
    probesRemoved.toVector.sorted.reverse.foreach { case ref: ProbeRef =>
      log.debug("probe {} retires", ref)
      val probeactor = probes(ref)
      probeactor.actor ! RetireProbe(lsn)
      retiredProbes.put(probeactor.actor, (ref,lsn))
    }
    // update existing probes and mark zombie probes
    val probesUpdated = probeSet.intersect(specSet)
    probesUpdated.foreach {
      case ref: ProbeRef if retiredProbes.contains(probes(ref).actor) =>
        zombieProbes.add(ref)
      case ref: ProbeRef =>
        val probeSpec = findProbeSpec(registration, ref.path)
        val directChildren = specSet.filter { _.parentOption match {
          case Some(parent) => parent == ref
          case None => false
        }}
        val ProbeActor(prevSpec, actor) = probes(ref)
        val factory = ProbeBehavior.extensions(probeSpec.probeType).configure(probeSpec.properties)
        probes = probes + (ref -> ProbeActor(probeSpec, actor))
        actor ! ChangeProbe(probeSpec.probeType, probeSpec.policy, factory, directChildren, lsn)
    }
  }

  /**
   *
   */
  def findMatching(uri: URI, paths: Option[Set[String]]): Set[(ProbeRef,ProbeActor)] = paths match {
    case None =>
      probes.toSet
    case Some(_paths) =>
      val parser = new ProbeMatcherParser()
      val matchers = _paths.map(path => parser.parseProbeMatcher(uri.toString + path))
      probes.flatMap { case matching @ (ref,actor) =>
        matchers.collectFirst {
          case matcher if matcher.matches(ref) =>
            matching
        }
      }.toSet
  }
}

object ProbeSystem {
  def props(services: ActorRef) = Props(classOf[ProbeSystem], services)

  case class ProbeActor(spec: ProbeSpec, actor: ActorRef)

  sealed trait State
  case object SystemIncubating extends State
  case object SystemRegistering extends State
  case object SystemInitializing extends State
  case object SystemRunning extends State
  case object SystemUpdating extends State
  case object SystemRetiring extends State
  case object SystemRetired extends State
  case object SystemFailed extends State

  sealed trait Data
  case object SystemWaiting extends Data
  case class SystemInitializing(uri: URI) extends Data
  case class SystemRegistering(op: RegisterProbeSystem, sender: ActorRef) extends Data
  case class SystemRunning(uri: URI, registration: ProbeRegistration, lsn: Long) extends Data
  case class SystemUpdating(op: UpdateProbeSystem, sender: ActorRef, prev: SystemRunning) extends Data
  case class SystemRetiring(op: RetireProbeSystem, sender: ActorRef, prev: SystemRunning) extends Data
  case class SystemRetired(uri: URI, registration: ProbeRegistration, lsn: Long) extends Data
  case class SystemError(ex: Throwable) extends Data
}

case class ChangeProbe(probeType: String, policy: ProbePolicy, factory: ProcessorFactory, children: Set[ProbeRef], lsn: Long)
case class RetireProbe(lsn: Long)

/**
 *
 */
sealed trait ProbeSystemOperation extends ServiceOperation { val uri: URI }
sealed trait ProbeSystemCommand extends ServiceCommand with ProbeSystemOperation
sealed trait ProbeSystemQuery extends ServiceQuery with ProbeSystemOperation
case class ProbeSystemOperationFailed(op: ProbeSystemOperation, failure: Throwable) extends ServiceOperationFailed

case class RegisterProbeSystem(uri: URI, registration: ProbeRegistration) extends ProbeSystemCommand
case class RegisterProbeSystemResult(op: RegisterProbeSystem, lsn: Long)

case class UpdateProbeSystem(uri: URI, registration: ProbeRegistration) extends ProbeSystemCommand
case class UpdateProbeSystemResult(op: UpdateProbeSystem, lsn: Long)

case class RetireProbeSystem(uri: URI) extends ProbeSystemCommand
case class RetireProbeSystemResult(op: RetireProbeSystem, lsn: Long)

case class ReviveProbeSystem(uri: URI) extends ProbeSystemQuery
case class ReviveProbeSystemResult(op: ReviveProbeSystem, lsn: Long)

case class DescribeProbeSystem(uri: URI) extends ProbeSystemQuery
case class DescribeProbeSystemResult(op: DescribeProbeSystem, registration: ProbeRegistration, lsn: Long)

case class MatchProbeSystem(uri: URI, matchers: Set[ProbeMatcher]) extends ProbeSystemQuery
case class MatchProbeSystemResult(op: MatchProbeSystem, refs: Set[ProbeRef])
