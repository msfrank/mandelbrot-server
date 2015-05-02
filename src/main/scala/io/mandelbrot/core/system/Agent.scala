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
import scala.collection.mutable

import io.mandelbrot.core._
import io.mandelbrot.core.model._
import io.mandelbrot.core.registry._
import io.mandelbrot.core.metrics.MetricsBus

/**
 * the Agent manages a collection of Probes underneath a URI.  the Agent
 * is responsible for adding and removing checks when the registration changes, as well
 * as updating checks when policy changes.  lastly, the Agent acts as an endpoint
 * for commands and queries operating on sets of checks in the system.
 */
class Agent(services: ActorRef) extends LoggingFSM[Agent.State,Agent.Data] with Stash {
  import Agent._

  // config
  val settings = ServerConfig(context.system).settings

  // state
  var checks: Map[CheckId,CheckActor] = Map.empty
  val retiredChecks = new mutable.HashMap[ActorRef,(CheckId,Long)]
  val zombieChecks = new mutable.HashSet[CheckId]
  val metricsBus = new MetricsBus()

  override def preStart(): Unit = {
    startWith(SystemIncubating, SystemWaiting)
    initialize()
  }

  when(SystemIncubating) {

    case Event(op: RegisterAgent, _) =>
      services ! CreateRegistration(op.agentId, op.registration)
      goto(SystemRegistering) using SystemRegistering(op, sender())

    case Event(revive: ReviveAgent, _) =>
      services ! GetRegistration(revive.agentId)
      goto(SystemInitializing) using SystemInitializing(revive.agentId)
      
    case Event(unhandled, _) =>
      throw new IllegalStateException("illegal message %s while in Incubating state".format(unhandled))
  }

  when(SystemRegistering) {

    case Event(result: CreateRegistrationResult, state: SystemRegistering) =>
      state.sender ! RegisterProbeSystemResult(state.op, result.metadata)
      goto(SystemRunning) using SystemRunning(state.op.agentId, result.op.registration, result.metadata.lsn)

    case Event(failure: RegistryServiceOperationFailed, state: SystemRegistering) =>
      state.sender ! failure
      goto(SystemFailed) using SystemError(failure.failure)

    case Event(op: AgentOperation, _) =>
      stash()
      stay()

    case Event(op: ProbeOperation, _) =>
      stash()
      stay()
  }

  when(SystemInitializing) {

    case Event(result: GetRegistrationResult, state: SystemInitializing) =>
      goto(SystemRunning) using SystemRunning(state.agentId, result.registration, result.lsn)

    case Event(failure: RegistryServiceOperationFailed, state: SystemInitializing) =>
      goto(SystemFailed) using SystemError(failure.failure)

    case Event(op: AgentOperation, _) =>
      stash()
      stay()

    case Event(op: ProbeOperation, _) =>
      stash()
      stay()
  }

  onTransition {
    case _ -> SystemRunning => nextStateData match {
      case state: SystemRunning =>
        log.debug("configuring probe system {}", state.agentId)
        unstashAll()
        applyProbeRegistration(state.agentId, state.registration, state.lsn)
      case _ =>
    }
  }

  when (SystemRunning) {

    /* get the Agent spec */
    case Event(query: DescribeAgent, state: SystemRunning) =>
      stay() replying DescribeProbeSystemResult(query, state.registration, state.lsn)

    case Event(query: MatchAgent, state: SystemRunning) =>
      if (query.matchers.nonEmpty) {
        val matchingRefs = checks.keys.flatMap { case checkId =>
          val probeRef = ProbeRef(state.agentId, checkId)
          query.matchers.collectFirst { case matcher if matcher.matches(probeRef.checkId) => probeRef }
        }.toSet
        stay() replying MatchProbeSystemResult(query, matchingRefs)
      } else stay() replying MatchProbeSystemResult(query, checks.keySet.map(checkId => ProbeRef(state.agentId, checkId)))

    case Event(op: RegisterAgent, state: SystemRunning) =>
      stay() replying ProbeSystemOperationFailed(op, ApiException(Conflict))

    /* update checks */
    case Event(op: UpdateAgent, state: SystemRunning) =>
      goto(SystemUpdating) using SystemUpdating(op, sender(), state)

    /* retire all running checks */
    case Event(op: RetireAgent, state: SystemRunning) =>
      goto(SystemRetiring) using SystemRetiring(op, sender(), state)

    /* ignore probe status from top level checks */
    case Event(status: ProbeStatus, state: SystemRunning) =>
      stay()

    /* forward probe operations to the specified probe */
    case Event(op: ProbeOperation, state: SystemRunning) =>
      checks.get(op.probeRef.checkId) match {
        case Some(probeActor: CheckActor) =>
          probeActor.actor.forward(op)
        case None =>
          sender() ! ProbeOperationFailed(op, ApiException(ResourceNotFound))
      }
      stay()

    /* handle notifications which have been passed up from Probe */
    case Event(notification: NotificationEvent, state: SystemRunning) =>
      services ! notification
      stay()

    /* clean up retired checks, reanimate zombie checks */
    case Event(Terminated(actorRef), state: SystemRunning) =>
      val (probeRef,lsn) = retiredChecks(actorRef)
      checks = checks - probeRef
      retiredChecks.remove(actorRef)
      if (zombieChecks.contains(probeRef)) {
        zombieChecks.remove(probeRef)
        applyProbeRegistration(state.agentId, state.registration, state.lsn)
      } else
        log.debug("probe {} has been terminated", probeRef)
      stay()
  }

  onTransition {
    case _ -> SystemUpdating => nextStateData match  {
      case state: SystemUpdating =>
        services ! UpdateRegistration(state.op.agentId, state.op.registration, state.prev.lsn)
      case _ =>
    }
  }

  when(SystemUpdating) {
    case Event(result: UpdateRegistrationResult, state: SystemUpdating) =>
      state.sender ! UpdateProbeSystemResult(state.op, result.metadata)
      goto(SystemRunning) using SystemRunning(state.op.agentId, state.op.registration, result.metadata.lsn)
    case Event(failure: RegistryServiceOperationFailed, state: SystemUpdating) =>
      state.sender ! failure
      goto(SystemRunning) using state.prev
    case Event(_, state: SystemUpdating) =>
      stash()
      stay()
  }

  onTransition {
    case _ -> SystemRetiring => nextStateData match {
      case state: SystemRetiring =>
        services ! DeleteRegistration(state.op.agentId, state.prev.lsn)
      case _ =>
    }
  }

  when(SystemRetiring) {
    case Event(result: DeleteRegistrationResult, state: SystemRetiring) =>
      state.sender ! RetireProbeSystemResult(state.op, result.lsn)
      goto(SystemRetired) using SystemRetired(state.prev.agentId, state.prev.registration, result.lsn)
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
        checks.foreach {
          case (probeRef,probeActor) if !retiredChecks.contains(probeActor.actor) =>
            probeActor.actor ! RetireProbe(state.lsn)
            retiredChecks.put(probeActor.actor, (probeRef,state.lsn))
          case _ => // do nothing
        }
      case _ =>
      }
  }

  when(SystemRetired) {
    /* clean up retired checks, reanimate zombie checks */
    case Event(Terminated(actorRef), state: SystemRetired) =>
      val (probeRef,lsn) = retiredChecks(actorRef)
      checks = checks - probeRef
      retiredChecks.remove(actorRef)
      if (zombieChecks.contains(probeRef))
        zombieChecks.remove(probeRef)
      if (checks.nonEmpty) stay() else stop()
    /* system doesn't exist anymore, so return resource not found */
    case Event(op: AgentOperation, _) =>
      stay() replying ProbeSystemOperationFailed(op, ApiException(ResourceNotFound))
    case Event(op: ProbeOperation, _) =>
      stay() replying ProbeOperationFailed(op, ApiException(ResourceNotFound))
  }

  onTransition {
    case _ -> SystemFailed => unstashAll()
  }

  when(SystemFailed) {
    case Event(op: AgentOperation, state: SystemError) =>
      stop() replying ProbeSystemOperationFailed(op, state.ex)
    case Event(op: ProbeOperation, state: SystemError) =>
      stop() replying ProbeOperationFailed(op, state.ex)
    /* ignore any other messages */
    case _: Event => stay()
  }

  /**
   * given a registration, return the set of all check resources, including
   * the implicit containers.
   */
  def makeRegistrationSet(registration: AgentRegistration): Set[CheckId] = {
    var registrationSet = registration.checks.keySet
    for (resource <- registrationSet) {
      var parent = resource.parentOption
      while (parent.isDefined) {
        registrationSet = registrationSet + parent.get
        parent = parent.get.parentOption
      }
    }
    registrationSet
  }

  /**
   * apply the spec to the probe system, adding and removing checks as necessary
   */
  def applyProbeRegistration(agentId: AgentId, registration: AgentRegistration, lsn: Long): Unit = {

    val registrationSet = makeRegistrationSet(registration)
    val checkSet = checks.keySet

    // create a processor factory for each check which isn't in checkSet
    val checksAdded = new mutable.HashMap[CheckId,ProbeBehaviorExtension#DependentProcessorFactory]
    (registrationSet -- checkSet).toVector.sorted.foreach { case resource: Resource =>
      registration.checks.get(resource) match {
        case Some(checkSpec) =>
          val checkType = checkSpec.probeType
          val properties = checkSpec.properties
          checksAdded.put(resource, ProbeBehavior.extensions(checkType).configure(properties))
        case None =>
          checksAdded.put(resource, Agent.checkPlaceholder.configure(Map.empty))
      }
    }

    // create a processor factory for each check which has been updated
    val checksUpdated = new mutable.HashMap[CheckId,ProbeBehaviorExtension#DependentProcessorFactory]
    checkSet.intersect(registrationSet).foreach { case resource: Resource =>
      val checkSpec = registration.checks(resource)
      val checkType = checkSpec.probeType
      val properties = checkSpec.properties
      val factory = ProbeBehavior.extensions(checkType).configure(properties)
      checksUpdated.put(resource, factory)
    }

    // remove stale checks
    val probesRemoved = checkSet -- registrationSet
    probesRemoved.toVector.sorted.reverse.foreach { case resource: Resource =>
      log.debug("probe {} retires", resource)
      val CheckActor(_, _, actor) = checks(resource)
      actor ! RetireProbe(lsn)
      retiredChecks.put(actor, (resource,lsn))
    }

    // create check actors for each added check
    checksAdded.keys.toVector.sorted.foreach { case checkId: CheckId =>
      val probeRef = ProbeRef(agentId, checkId)
      val actor = checkId.parentOption match {
        case Some(parent) =>
          context.actorOf(Probe.props(probeRef, checks(parent).actor, services, metricsBus))
        case _ =>
          context.actorOf(Probe.props(probeRef, self, services, metricsBus))
      }
      log.debug("check {} joins {}", checkId, agentId)
      context.watch(actor)
      val checkSpec = registration.checks(checkId)
      val factory = checksAdded(checkId)
      checks = checks + (checkId -> CheckActor(checkSpec, factory, actor))
    }

    // update existing checks and mark zombie checks
    checksUpdated.toVector.foreach {
      case (resource, factory) if retiredChecks.contains(checks(resource).actor) =>
        zombieChecks.add(resource)
      case (resource, factory) =>
        val CheckActor(_, _, actor) = checks(resource)
        val checkSpec = registration.checks(resource)
        checks = checks + (resource -> CheckActor(checkSpec, factory, actor))
    }

    // configure all added and updated checks
    (checksAdded.keySet ++ checksUpdated.keySet).foreach { case checkId =>
      val CheckActor(checkSpec, factory, actor) = checks(checkId)
      val directChildren = registrationSet.filter { _.parentOption match {
        case Some(parent) => parent == checkId
        case None => false
      }}.map(childId => ProbeRef(agentId, childId))
      actor ! ChangeProbe(checkSpec.probeType, checkSpec.policy, factory, directChildren, lsn)
    }
  }
}

object Agent {
  def props(services: ActorRef) = Props(classOf[Agent], services)

  val checkPlaceholder = new ContainerProbe()

  case class CheckActor(spec: CheckSpec, factory: ProbeBehaviorExtension#DependentProcessorFactory, actor: ActorRef)

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
  case class SystemInitializing(agentId: AgentId) extends Data
  case class SystemRegistering(op: RegisterAgent, sender: ActorRef) extends Data
  case class SystemRunning(agentId: AgentId, registration: AgentRegistration, lsn: Long) extends Data
  case class SystemUpdating(op: UpdateAgent, sender: ActorRef, prev: SystemRunning) extends Data
  case class SystemRetiring(op: RetireAgent, sender: ActorRef, prev: SystemRunning) extends Data
  case class SystemRetired(agentId: AgentId, registration: AgentRegistration, lsn: Long) extends Data
  case class SystemError(ex: Throwable) extends Data
}

case class ReviveAgent(agentId: AgentId)
case class ChangeProbe(probeType: String, policy: CheckPolicy, factory: ProcessorFactory, children: Set[ProbeRef], lsn: Long)
case class RetireProbe(lsn: Long)

/**
 *
 */
sealed trait AgentOperation extends ServiceOperation { val agentId: AgentId }
sealed trait AgentCommand extends ServiceCommand with AgentOperation
sealed trait AgentQuery extends ServiceQuery with AgentOperation
case class ProbeSystemOperationFailed(op: AgentOperation, failure: Throwable) extends ServiceOperationFailed

case class RegisterAgent(agentId: AgentId, registration: AgentRegistration) extends AgentCommand
case class RegisterProbeSystemResult(op: RegisterAgent, metadata: AgentMetadata)

case class UpdateAgent(agentId: AgentId, registration: AgentRegistration) extends AgentCommand
case class UpdateProbeSystemResult(op: UpdateAgent, metadata: AgentMetadata)

case class RetireAgent(agentId: AgentId) extends AgentCommand
case class RetireProbeSystemResult(op: RetireAgent, lsn: Long)

case class DescribeAgent(agentId: AgentId) extends AgentQuery
case class DescribeProbeSystemResult(op: DescribeAgent, registration: AgentRegistration, lsn: Long)

case class MatchAgent(agentId: AgentId, matchers: Set[CheckMatcher]) extends AgentQuery
case class MatchProbeSystemResult(op: MatchAgent, refs: Set[ProbeRef])
