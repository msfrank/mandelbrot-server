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
import akka.contrib.pattern.ShardRegion
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import io.mandelbrot.core.metrics.MetricsBus
import org.joda.time.DateTime
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.collection.mutable
import java.net.{URL, URI}
import java.util.UUID

import io.mandelbrot.core.{ServerConfig, ResourceNotFound, ApiException}
import io.mandelbrot.core.registry._
import io.mandelbrot.core.notification.{ProbeNotification, NotificationEvent}
import io.mandelbrot.core.history._

/**
 * the ProbeSystem manages a collection of Probes underneath a URI.  the ProbeSystem
 * is responsible for adding and removing probes when the registration changes, as well
 * as updating probes when policy changes.  lastly, the ProbeSystem acts as an endpoint
 * for commands and queries operating on sets of probes in the system.
 */
class ProbeSystem(services: ActorRef) extends LoggingFSM[SystemFSMState,SystemFSMData] with Stash {
  import ProbeSystem._

  // config
  val settings = ServerConfig(context.system).settings

  // state
  var probes: Map[ProbeRef,ProbeActor] = Map.empty
  val retiredProbes = new mutable.HashMap[ActorRef,(ProbeRef,Long)]
  val zombieProbes = new mutable.HashSet[ProbeRef]
  val links = new mutable.HashMap[ProbeRef,ProbeLink]
  val metricsBus = new MetricsBus()

  override def preStart(): Unit = {
    startWith(SystemIncubating, SystemWaiting)
    initialize()
  }

  when(SystemIncubating) {
    case Event(op: ProbeSystemOperation, _) =>
      stash()
      goto(SystemInitializing) using SystemInitializing(op.uri)
      
    case Event(op: ProbeOperation, _) =>
      stash()
      goto(SystemInitializing) using SystemInitializing(op.probeRef.uri)

    case _: Event => stop()
  }
  
  onTransition {
    case SystemIncubating -> SystemInitializing => nextStateData match {
      case state: SystemInitializing => services ! GetProbeSystemEntry(state.uri)
      case _ =>
    }
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

    /* update probes */
    case Event(op: UpdateProbeSystem, state: SystemRunning) =>
      goto(SystemUpdating) using SystemUpdating(op, sender(), state)

    /* retire all running probes */
    case Event(op: RetireProbeSystem, state: SystemRunning) =>
      goto(SystemRetiring) using SystemRetiring(op, sender(), state)

    /* send status message to specified probe */
    case Event(message: StatusMessage, state: SystemRunning) =>
      probes.get(message.probeRef) match {
        case Some(probeActor: ProbeActor) =>
          probeActor.actor ! message
        case None =>
          log.warning("ignoring message {}: probe source is not known", message)
      }
      stay()

    /* send metrics message to all interested probes */
    case Event(message: MetricsMessage, state: SystemRunning) =>
      if (probes.contains(message.probeRef))
        metricsBus.publish(message)
      else
        log.warning("ignoring message {}: probe source is not known", message)
      stay()

    /* ignore probe status from top level probes */
    case Event(status: ProbeStatus, state: SystemRunning) =>
      stay()

    /* forward probe operations to the specified probe */
    case Event(op: ProbeOperation, state: SystemRunning) =>
      probes.get(op.probeRef) match {
        case Some(probeActor: ProbeActor) =>
          probeActor.actor.forward(op)
        case None =>
          sender() ! ProbeOperationFailed(op, new ApiException(ResourceNotFound))
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
      stay() replying ProbeSystemOperationFailed(op, new ApiException(ResourceNotFound))
    case Event(op: ProbeOperation, _) =>
      stay() replying ProbeOperationFailed(op, new ApiException(ResourceNotFound))
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
      val actor = ref.parentOption match {
        case Some(parent) if parent.path.nonEmpty =>
          context.actorOf(Probe.props(ref, probes(parent).actor, directChildren, probeSpec.policy, probeSpec.behavior, lsn, services, metricsBus))
        case _ =>
          context.actorOf(Probe.props(ref, self, directChildren, probeSpec.policy, probeSpec.behavior, lsn, services, metricsBus))
      }
      context.watch(actor)
      log.debug("probe {} joins", ref)
      probes = probes + (ref -> ProbeActor(probeSpec, actor))
      services ! ProbeMetadata(ref, registration.metadata)
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
        probes = probes + (ref -> ProbeActor(probeSpec, actor))
        if (probeSpec.behavior.getClass == prevSpec.behavior.getClass)
          actor ! UpdateProbe(directChildren, probeSpec.policy, probeSpec.behavior, lsn)
        else
          actor ! ChangeProbe(directChildren, probeSpec.policy, probeSpec.behavior, lsn)
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
  def props(services: ActorRef) =  Props(classOf[ProbeSystem], services)
  val idExtractor: ShardRegion.IdExtractor = {
    case op: ProbeSystemOperation => (op.uri.toString, op)
    case op: ProbeOperation => (op.probeRef.uri.toString, op)
  }
  val shardResolver: ShardRegion.ShardResolver = {
    case op: ProbeSystemOperation => op.uri.toString
    case op: ProbeOperation => op.probeRef.uri.toString
  }
  case class ProbeActor(spec: ProbeSpec, actor: ActorRef)
}

sealed trait SystemFSMState
case object SystemIncubating extends SystemFSMState
case object SystemInitializing extends SystemFSMState
case object SystemRunning extends SystemFSMState
case object SystemUpdating extends SystemFSMState
case object SystemRetiring extends SystemFSMState
case object SystemRetired extends SystemFSMState
case object SystemFailed extends SystemFSMState

sealed trait SystemFSMData
case object SystemWaiting extends SystemFSMData
case class SystemInitializing(uri: URI) extends SystemFSMData
case class SystemRunning(uri: URI, registration: ProbeRegistration, lsn: Long) extends SystemFSMData
case class SystemUpdating(op: UpdateProbeSystem, sender: ActorRef, prev: SystemRunning) extends SystemFSMData
case class SystemRetiring(op: RetireProbeSystem, sender: ActorRef, prev: SystemRunning) extends SystemFSMData
case class SystemRetired(uri: URI, registration: ProbeRegistration, lsn: Long) extends SystemFSMData
case class SystemError(ex: Throwable) extends SystemFSMData

case class UpdateProbe(children: Set[ProbeRef], policy: ProbePolicy, behavior: ProbeBehavior, lsn: Long)
case class ChangeProbe(children: Set[ProbeRef], policy: ProbePolicy, behavior: ProbeBehavior, lsn: Long)
case class RetireProbe(lsn: Long)

/* describes a link to a probe subtree from a different probe system */
case class ProbeLink(localRef: ProbeRef, remoteUrl: URI, remoteMatch: String)

/**
 *
 */
sealed trait ProbeSystemOperation { val uri: URI }
sealed trait ProbeSystemCommand extends ProbeSystemOperation
sealed trait ProbeSystemQuery extends ProbeSystemOperation
case class ProbeSystemOperationFailed(op: ProbeSystemOperation, failure: Throwable)

case class UpdateProbeSystem(uri: URI, registration: ProbeRegistration) extends ProbeSystemCommand
case class UpdateProbeSystemResult(op: UpdateProbeSystem, lsn: Long)

case class RetireProbeSystem(uri: URI) extends ProbeSystemCommand
case class RetireProbeSystemResult(op: RetireProbeSystem, lsn: Long)

case class ReviveProbeSystem(uri: URI) extends ProbeSystemQuery
case class ReviveProbeSystemResult(op: ReviveProbeSystem, lsn: Long)

case class DescribeProbeSystem(uri: URI) extends ProbeSystemQuery
case class DescribeProbeSystemResult(op: DescribeProbeSystem, registration: ProbeRegistration, lsn: Long)
