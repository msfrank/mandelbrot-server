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
import org.joda.time.{DateTimeZone, DateTime}
import scala.collection.mutable
import scala.concurrent.duration._

import io.mandelbrot.core._
import io.mandelbrot.core.model._
import io.mandelbrot.core.registry._
import io.mandelbrot.core.metrics.MetricsBus

/**
 * the Agent manages a collection of Checks underneath a URI.  the Agent
 * is responsible for adding and removing checks when the registration changes, as well
 * as updating checks when policy changes.  lastly, the Agent acts as an endpoint
 * for commands and queries operating on sets of checks in the system.
 */
class Agent(services: ActorRef) extends LoggingFSM[Agent.State,Agent.Data] with Stash {
  import Agent._
  import context.dispatcher

  // config
  val settings = ServerConfig(context.system).settings
  val retirementPeriod = 1.days     // FIXME: define this in agent registration
  val activeRetirement = 5.minutes  // FIXME: define this in ServerConfig

  // state
  var checks: Map[CheckId,CheckActor] = Map.empty
  val retiredChecks = new mutable.HashMap[ActorRef,(CheckId,Long)]
  val zombieChecks = new mutable.HashSet[CheckId]
  val metricsBus = new MetricsBus()
  var generation: Long = 0
  var lsn: Long = 0

  override def preStart(): Unit = {
    startWith(SystemIncubating, SystemWaiting)
    initialize()
  }

  when(SystemIncubating) {

    case Event(op: RegisterAgent, _) =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val metadata = AgentMetadata(op.agentId, generation + 1, timestamp, timestamp, None)
      services ! CreateRegistration(op.agentId, op.registration, metadata, lsn + 1)
      goto(SystemRegistering) using SystemRegistering(op, sender())

    case Event(revive: ReviveAgent, _) =>
      services ! GetRegistration(revive.agentId)
      goto(SystemInitializing) using SystemInitializing(revive.agentId)
      
    case Event(op: AgentOperation, _) =>
      stash()
      stay()

    case Event(op: CheckOperation, _) =>
      stash()
      stay()
  }

  when(SystemRegistering) {

    case Event(result: CreateRegistrationResult, state: SystemRegistering) =>
      state.sender ! RegisterAgentResult(state.op, result.metadata)
      generation = result.metadata.generation
      lsn = result.op.lsn
      goto(SystemRunning) using SystemRunning(state.op.agentId, result.op.registration, result.metadata)

    case Event(failure: RegistryServiceOperationFailed, state: SystemRegistering) =>
      state.sender ! failure
      goto(SystemFailed) using SystemError(failure.failure)

    case Event(op: AgentOperation, _) =>
      stash()
      stay()

    case Event(op: CheckOperation, _) =>
      stash()
      stay()
  }

  when(SystemInitializing) {

    case Event(result: GetRegistrationResult, state: SystemInitializing) =>
      generation = result.metadata.generation
      lsn = result.lsn
      if (result.metadata.expires.isDefined)
        goto(SystemRetired) using SystemRetired(state.agentId, result.registration, result.metadata)
      else
        goto(SystemRunning) using SystemRunning(state.agentId, result.registration, result.metadata)

    case Event(failure: RegistryServiceOperationFailed, state: SystemInitializing) =>
      goto(SystemFailed) using SystemError(failure.failure)

    case Event(op: AgentOperation, _) =>
      stash()
      stay()

    case Event(op: CheckOperation, _) =>
      stash()
      stay()
  }

  onTransition {
    case _ -> SystemRunning => nextStateData match {
      case state: SystemRunning =>
        log.debug("configuring check system {}", state.agentId)
        unstashAll()
        applyCheckRegistration(state.agentId, state.registration, lsn)
      case _ =>
    }
  }

  when (SystemRunning) {

    /* get the Agent spec */
    case Event(query: DescribeAgent, state: SystemRunning) =>
      stay() replying DescribeAgentResult(query, state.registration, state.metadata)

    case Event(query: MatchAgent, state: SystemRunning) =>
      if (query.matchers.nonEmpty) {
        val matchingRefs = checks.keys.flatMap { case checkId =>
          val checkRef = CheckRef(state.agentId, checkId)
          query.matchers.collectFirst { case matcher if matcher.matches(checkRef.checkId) => checkRef }
        }.toSet
        stay() replying MatchAgentResult(query, matchingRefs)
      } else stay() replying MatchAgentResult(query, checks.keySet.map(checkId => CheckRef(state.agentId, checkId)))

    case Event(op: RegisterAgent, state: SystemRunning) =>
      stay() replying AgentOperationFailed(op, ApiException(Conflict))

    /* update checks */
    case Event(op: UpdateAgent, state: SystemRunning) =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val metadata = state.metadata.copy(lastUpdate = timestamp)
      val update = UpdateRegistration(op.agentId, op.registration, metadata, lsn + 1)
      goto(SystemUpdating) using SystemUpdating(op, sender(), update, state)

    /* retire all running checks */
    case Event(op: RetireAgent, state: SystemRunning) =>
      val tombstone = DateTime.now(DateTimeZone.UTC).plus(retirementPeriod.toMillis)
      val metadata = state.metadata.copy(expires = Some(tombstone))
      val retire = RetireRegistration(op.agentId, state.registration, metadata, lsn + 1)
      goto(SystemRetiring) using SystemRetiring(op, sender(), retire, state)

    /* ignore check status from top level checks */
    case Event(status: CheckStatus, state: SystemRunning) =>
      stay()

    /* forward check operations to the specified check */
    case Event(op: CheckOperation, state: SystemRunning) =>
      checks.get(op.checkRef.checkId) match {
        case Some(checkActor: CheckActor) =>
          checkActor.actor.forward(op)
        case None =>
          sender() ! CheckOperationFailed(op, ApiException(ResourceNotFound))
      }
      stay()

    /* handle notifications which have been passed up from Check */
    case Event(notification: NotificationEvent, state: SystemRunning) =>
      services ! notification
      stay()

    /* clean up retired checks, reanimate zombie checks */
    case Event(Terminated(actorRef), state: SystemRunning) =>
      val (checkRef,lsn) = retiredChecks(actorRef)
      checks = checks - checkRef
      retiredChecks.remove(actorRef)
      if (zombieChecks.contains(checkRef)) {
        zombieChecks.remove(checkRef)
        applyCheckRegistration(state.agentId, state.registration, lsn)
      } else
        log.debug("check {} has been terminated", checkRef)
      stay()
  }

  onTransition {
    case _ -> SystemUpdating => nextStateData match  {
      case state: SystemUpdating =>
        services ! state.update
      case _ =>
    }
  }

  when(SystemUpdating) {
    case Event(result: UpdateRegistrationResult, state: SystemUpdating) =>
      state.sender ! UpdateAgentResult(state.op, state.update.metadata)
      goto(SystemRunning) using SystemRunning(state.op.agentId, state.op.registration, state.update.metadata)
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
        services ! state.retire
      case _ =>
    }
  }

  /*
   * RETIRING is a transitional state from RUNNING to RETIRED.  in RETIRING state
   * we tell the registry manager to mark us as retired, then upon confirmation we
   * write a tombstone entry and retire all checks.  once the tombstone has been
   * written and all checks are terminated, we transition to RETIRED.
   *
   * If registry manager fails to mark as as retired, then we return failure to the
   * client and transition back to RUNNING.
   */
  when(SystemRetiring) {

    /* we have marked agent as retired */
    case Event(result: RetireRegistrationResult, state: SystemRetiring) =>
      state.sender ! RetireAgentResult(state.op, result.op.metadata)
      services ! PutTombstone(state.retire.agentId, state.retire.metadata.generation, state.retire.metadata.expires.get)
      stay()

    /* we have written the tombstone */
    case Event(result: PutTombstoneResult, state: SystemRetiring) =>
      checks.foreach {
        case (checkRef,checkActor) if !retiredChecks.contains(checkActor.actor) =>
          checkActor.actor ! RetireCheck(lsn)
          retiredChecks.put(checkActor.actor, (checkRef,lsn))
        case _ => // do nothing
      }
      stay()

    /* check actor has terminated */
    case Event(Terminated(actorRef), state: SystemRetiring) =>
      val (checkRef,lsn) = retiredChecks(actorRef)
      checks = checks - checkRef
      retiredChecks.remove(actorRef)
      if (zombieChecks.contains(checkRef))
        zombieChecks.remove(checkRef)
      if (checks.isEmpty) {
        goto(SystemRetired) using SystemRetired(state.retire.agentId, state.retire.registration, state.retire.metadata)
      } else stay()

    /* RetireRegistration failed */
    case Event(failure @ RegistryServiceOperationFailed(op: RetireRegistration, _), state: SystemRetiring) =>
      state.sender ! failure
      goto(SystemRunning) using state.prev // go back to RUNNING

    /* PutTombstone failed */
    case Event(failure @ RegistryServiceOperationFailed(op: PutTombstone, _), state: SystemRetiring) =>
      throw new IllegalStateException("failed to put tombstone for {} with generation {}".format(op.agentId, op.generation))

    /* we stash any other message while in transitional state */
    case Event(_, state: SystemRetiring) =>
      stash()
      stay()
  }

  onTransition {
    case _ -> SystemRetired => nextStateData match {
      case state: SystemRetired =>
        unstashAll()
        context.system.scheduler.scheduleOnce(activeRetirement, self, StopAgent)
      case _ =>
    }
  }

  /*
   * when in RETIRED state the agent responds to queries but does not write any
   * data.  if asked to register a new generation, then we transition to INCUBATING.
   * after a certain period of time, we terminate the actor because it is unlikely
   * that the client will query the agent frequently.
   */
  when(SystemRetired) {

    /* active retirement period has ended, stop agent */
    case Event(StopAgent, state: SystemRetired) =>
      stop()

    /* register a new generation of the agent */
    case Event(op: RegisterAgent, state: SystemRetired) =>
      self.forward(op)
      goto(SystemIncubating) using SystemWaiting

    case Event(op: DescribeAgent, state: SystemRetired) =>
      stay() replying DescribeAgentResult(op, state.registration, state.metadata)

    case Event(op: MatchAgent, state: SystemRetired) =>
      stay() replying AgentOperationFailed(op, ApiException(ResourceNotFound))

    /* agent doesn't exist anymore, so return resource not found */
    case Event(op: AgentOperation, _) =>
      stay() replying AgentOperationFailed(op, ApiException(ResourceNotFound))

    /* agent doesn't exist anymore, so return resource not found */
    case Event(op: CheckOperation, _) =>
      stay() replying CheckOperationFailed(op, ApiException(ResourceNotFound))
  }

  onTransition {
    case _ -> SystemFailed => unstashAll()
  }

  when(SystemFailed) {
    case Event(op: AgentOperation, state: SystemError) =>
      stop() replying AgentOperationFailed(op, state.ex)
    case Event(op: CheckOperation, state: SystemError) =>
      stop() replying CheckOperationFailed(op, state.ex)
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
   * apply the spec to the check system, adding and removing checks as necessary
   */
  def applyCheckRegistration(agentId: AgentId, registration: AgentRegistration, lsn: Long): Unit = {

    val registrationSet = makeRegistrationSet(registration)
    val checkSet = checks.keySet

    // create a processor factory for each check which isn't in checkSet
    val checksAdded = new mutable.HashMap[CheckId,(CheckSpec,CheckBehaviorExtension#DependentProcessorFactory)]
    (registrationSet -- checkSet).toVector.sorted.foreach { case checkId: CheckId =>
      registration.checks.get(checkId) match {
        case Some(checkSpec) =>
          val checkType = checkSpec.checkType
          val properties = checkSpec.properties
          val factory = CheckBehavior.extensions(checkType).configure(properties)
          checksAdded.put(checkId, (checkSpec,factory))
        case None =>
          val factory = Agent.placeholderCheck.configure(Map.empty)
          checksAdded.put(checkId, (placeholderCheckSpec,factory))
      }
    }

    // create a processor factory for each check which has been updated
    val checksUpdated = new mutable.HashMap[CheckId,(CheckSpec,CheckBehaviorExtension#DependentProcessorFactory)]
    checkSet.intersect(registrationSet).foreach { case checkId: CheckId =>
      registration.checks.get(checkId) match {
        case Some(checkSpec) =>
          val checkType = checkSpec.checkType
          val properties = checkSpec.properties
          val factory = CheckBehavior.extensions(checkType).configure(properties)
          checksUpdated.put(checkId, (checkSpec,factory))
        case None =>
          val factory = Agent.placeholderCheck.configure(Map.empty)
          checksUpdated.put(checkId, (placeholderCheckSpec,factory))
      }
    }

    // remove stale checks
    val checksRemoved = checkSet -- registrationSet
    checksRemoved.toVector.sorted.reverse.foreach { case checkId: CheckId =>
      log.debug("check {} retires", checkId)
      val CheckActor(_, _, actor) = checks(checkId)
      actor ! RetireCheck(lsn)
      retiredChecks.put(actor, (checkId,lsn))
    }

    // create check actors for each added check
    checksAdded.keys.toVector.sorted.foreach { checkId =>
      val (checkSpec, factory) = checksAdded(checkId)
      val checkRef = CheckRef(agentId, checkId)
      val actor = checkId.parentOption match {
        case Some(parent) =>
          context.actorOf(Check.props(checkRef, checks(parent).actor, services, metricsBus))
        case _ =>
          context.actorOf(Check.props(checkRef, self, services, metricsBus))
      }
      log.debug("check {} joins {}", checkId, agentId)
      context.watch(actor)
      checks = checks + (checkId -> CheckActor(checkSpec, factory, actor))
    }

    // update existing checks and mark zombie checks
    checksUpdated.keys.toVector.foreach { checkId =>
      val (checkSpec, factory) = checksUpdated(checkId)
      if (retiredChecks.contains(checks(checkId).actor)) {
        zombieChecks.add(checkId)
      } else {
        val CheckActor(_, _, actor) = checks(checkId)
        checks = checks + (checkId -> CheckActor(checkSpec, factory, actor))
      }
    }

    // configure all added and updated checks
    (checksAdded.keySet ++ checksUpdated.keySet).foreach { case checkId =>
      val CheckActor(checkSpec, factory, actor) = checks(checkId)
      val directChildren = registrationSet.filter { _.parentOption match {
        case Some(parent) => parent == checkId
        case None => false
      }}.map(childId => CheckRef(agentId, childId))
      actor ! ChangeCheck(checkSpec.checkType, checkSpec.policy, factory, directChildren, lsn)
    }
  }
}

object Agent {
  def props(services: ActorRef) = Props(classOf[Agent], services)

  val placeholderCheck = new ContainerCheck()
  val placeholderCheckSpec = CheckSpec("io.mandelbrot.core.system.ContainerCheck",
    CheckPolicy(0.seconds, 5.minutes, 5.minutes, 5.minutes, None), Map.empty, Map.empty
  )
  
  case class CheckActor(spec: CheckSpec, factory: CheckBehaviorExtension#DependentProcessorFactory, actor: ActorRef)

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
  case class SystemRunning(agentId: AgentId, registration: AgentRegistration, metadata: AgentMetadata) extends Data
  case class SystemUpdating(op: UpdateAgent, sender: ActorRef, update: UpdateRegistration, prev: SystemRunning) extends Data
  case class SystemRetiring(op: RetireAgent, sender: ActorRef, retire: RetireRegistration, prev: SystemRunning) extends Data
  case class SystemRetired(agentId: AgentId, registration: AgentRegistration, metadata: AgentMetadata) extends Data
  case class SystemError(ex: Throwable) extends Data

  case object StopAgent
}

case class ReviveAgent(agentId: AgentId)
case class ChangeCheck(checkType: String, policy: CheckPolicy, factory: ProcessorFactory, children: Set[CheckRef], lsn: Long)
case class RetireCheck(lsn: Long)

/**
 *
 */
sealed trait AgentOperation extends ServiceOperation { val agentId: AgentId }
sealed trait AgentCommand extends ServiceCommand with AgentOperation
sealed trait AgentQuery extends ServiceQuery with AgentOperation
case class AgentOperationFailed(op: AgentOperation, failure: Throwable) extends ServiceOperationFailed

case class RegisterAgent(agentId: AgentId, registration: AgentRegistration) extends AgentCommand
case class RegisterAgentResult(op: RegisterAgent, metadata: AgentMetadata)

case class UpdateAgent(agentId: AgentId, registration: AgentRegistration) extends AgentCommand
case class UpdateAgentResult(op: UpdateAgent, metadata: AgentMetadata)

case class RetireAgent(agentId: AgentId) extends AgentCommand
case class RetireAgentResult(op: RetireAgent, metadata: AgentMetadata)

case class DeleteAgent(agentId: AgentId, generation: Long) extends AgentCommand
case class DeleteAgentResult(op: DeleteAgent)

case class DescribeAgent(agentId: AgentId) extends AgentQuery
case class DescribeAgentResult(op: DescribeAgent, registration: AgentRegistration, metadata: AgentMetadata)

case class MatchAgent(agentId: AgentId, matchers: Set[CheckMatcher]) extends AgentQuery
case class MatchAgentResult(op: MatchAgent, refs: Set[CheckRef])
