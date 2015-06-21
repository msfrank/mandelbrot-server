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

package io.mandelbrot.core.agent

import akka.actor._
import io.mandelbrot.core._
import io.mandelbrot.core.metrics.MetricsBus
import io.mandelbrot.core.model._
import io.mandelbrot.core.registry._
import io.mandelbrot.core.system._
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.mutable
import scala.concurrent.duration._

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
  val updatingTimeout = 5.seconds   // FIXME: define this in ServerConfig
  val retiringTimeout = 5.seconds   // FIXME: define this in ServerConfig

  // state
  var checks: Map[CheckId,CheckActor] = Map.empty
  val retiredChecks = new mutable.HashMap[ActorRef,(CheckId,Long)]
  val zombieChecks = new mutable.HashSet[CheckId]
  val metricsBus = new MetricsBus()
  var generation: Long = 0
  var lsn: Long = 0

  override def preStart(): Unit = {
    startWith(AgentIncubating, NoData)
    initialize()
  }

  /*
   * The agent always starts in INCUBATING state.  it waits for the first message,
   * and uses the message type to determine whether to transition to REGISTERING or
   * INITIALIZING.  Any other message is stashed.
   */
  when(AgentIncubating) {

    case Event(op: RegisterAgent, _) =>
      goto(AgentRegistering) using AgentRegistering(op, sender())

    case Event(op: ReviveAgent, _) =>
      goto(AgentInitializing) using AgentInitializing(op, sender())

    /* stash any agent operations for later */
    case Event(op: AgentOperation, _) =>
      stash()
      stay()

    /* stash any check operations for later */
    case Event(op: CheckOperation, _) =>
      stash()
      stay()
  }

  onTransition {
    /* request the current registration when we are transitioning to REGISTERING */
    case _ -> AgentRegistering =>
      val state = nextStateData.asInstanceOf[AgentRegistering]
      services ! GetRegistration(state.op.agentId)
  }

  /*
   * The agent transitions to REGISTERING state when the first message received was
   * RegisterAgent.  If the agent exists but needs recovery, then we transition to
   * RECOVERING, and configure the recovery state to loop us back to REGISTERING state
   * once recovery finishes.  Otherwise we start the agent registration task and wait
   * for the registration complete message.  If the registration task fails, then the
   * supervision strategy will catch the failure.
   */
  when(AgentRegistering) {

    /* the agent already exists and has uncommitted changes */
    case Event(result: GetRegistrationResult, state: AgentRegistering) if !result.committed =>
      val commit = CommitRegistration(state.op.agentId, result.registration, result.metadata, result.lsn)
      goto(AgentRecovering) using AgentRecovering(commit, AgentRegistering, state)

    /* the agent already exists */
    case Event(result: GetRegistrationResult, state: AgentRegistering) =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val metadata = result.metadata.copy(generation = result.metadata.generation + 1, lastUpdate = timestamp)
      services ! PutRegistration(state.op.agentId, state.op.registration, metadata, result.lsn + 1)
      stay()

    /* the agent doesn't exist */
    case Event(RegistryServiceOperationFailed(_: GetRegistration, ApiException(ResourceNotFound)), state: AgentRegistering) =>
      context.actorOf(RegisterAgentTask.props(state.op, state.sender, generation + 1, lsn + 1, services))
      stay()

    /* we successfully registered the agent */
    case Event(result: RegisterAgentTaskComplete, state: AgentRegistering) =>
      generation = result.metadata.generation
      lsn = result.lsn
      goto(AgentRunning) using AgentRunning(state.op.agentId, result.registration, result.metadata)

    /* any unhandled failure */
    case Event(failure: RegistryServiceOperationFailed, state: AgentRegistering) =>
      state.sender ! AgentOperationFailed(state.op, failure.failure)
      throw failure.failure

    /* stash any agent operations for later */
    case Event(op: AgentOperation, _) =>
      stash()
      stay()

    /* stash any check operations for later */
    case Event(op: CheckOperation, _) =>
      stash()
      stay()
  }

  onTransition {
    /* request the current registration when we are transitioning to INITIALIZING */
    case _ -> AgentInitializing =>
      val state = nextStateData.asInstanceOf[AgentInitializing]
      services ! GetRegistration(state.op.agentId)
  }

  /*
   * The agent transitions to INITIALIZING state when the first message received was
   * ReviveAgent.
   */
  when(AgentInitializing) {

    /* the agent exists and has uncommitted changes */
    case Event(result: GetRegistrationResult, state: AgentInitializing) if !result.committed =>
      val commit = CommitRegistration(state.op.agentId, result.registration, result.metadata, result.lsn)
      goto(AgentRecovering) using AgentRecovering(commit, AgentInitializing, state)

    /* the agent exists and is retired */
    case Event(result: GetRegistrationResult, state: AgentInitializing) if result.metadata.expires.isDefined =>
      generation = result.metadata.generation
      lsn = result.lsn
      goto(AgentRetired) using AgentRetired(state.op.agentId, result.registration, result.metadata)

    /* the agent exists and is active */
    case Event(result: GetRegistrationResult, state: AgentInitializing) =>
      generation = result.metadata.generation
      lsn = result.lsn
      goto(AgentRunning) using AgentRunning(state.op.agentId, result.registration, result.metadata)

    /* any unhandled failure */
    case Event(failure: RegistryServiceOperationFailed, state: AgentInitializing) =>
      state.sender ! AgentOperationFailed(state.op, failure.failure)
      throw failure.failure

    /* stash any agent operations for later */
    case Event(op: AgentOperation, _) =>
      stash()
      stay()

    /* stash any check operations for later */
    case Event(op: CheckOperation, _) =>
      stash()
      stay()
  }

  onTransition {
    /* start the agent recovery task when we are transitioning to RECOVERING */
    case _ -> AgentRecovering =>
      val state = nextStateData.asInstanceOf[AgentRecovering]
      context.actorOf(RecoverAgentTask.props(state.commit, services))
  }

  /*
   * When the agent transitions to RECOVERING state, the recovery task has already
   * been started, so we simply wait until the recovery complete message is received.
   * if the task fails, then supervision will catch the exception.
   */
  when(AgentRecovering) {

    case Event(result: RecoverAgentTaskComplete, state: AgentRecovering) =>
      goto(state.nextState) using state.nextData

    /* stash any agent operations for later */
    case Event(op: AgentOperation, _) =>
      stash()
      stay()

    /* stash any check operations for later */
    case Event(op: CheckOperation, _) =>
      stash()
      stay()
  }

  onTransition {
    /* apply the current registration and unstash pending operations */
    case _ -> AgentRunning =>
      val state = nextStateData.asInstanceOf[AgentRunning]
      log.debug("configuring agent {}", state.agentId)
      applyAgentRegistration(state.agentId, state.registration, lsn)
      retiredChecks.keys.foreach(_ ! PoisonPill)
      unstashAll()
  }

  /*
   *
   */
  when (AgentRunning) {

    /* get the Agent spec */
    case Event(query: DescribeAgent, state: AgentRunning) =>
      stay() replying DescribeAgentResult(query, state.registration, state.metadata)

    /* return all checks which match the query */
    case Event(query: MatchAgent, state: AgentRunning) =>
      if (query.matchers.nonEmpty) {
        val matchingRefs = checks.keys.flatMap { case checkId =>
          val checkRef = CheckRef(state.agentId, checkId)
          query.matchers.collectFirst { case matcher if matcher.matches(checkRef.checkId) => checkRef }
        }.toSet
        stay() replying MatchAgentResult(query, matchingRefs)
      } else stay() replying MatchAgentResult(query, checks.keySet.map(checkId => CheckRef(state.agentId, checkId)))

    /* return Conflict because the agent already exists */
    case Event(op: RegisterAgent, state: AgentRunning) =>
      stay() replying AgentOperationFailed(op, ApiException(Conflict))

    /* apply updated agent registration */
    case Event(op: UpdateAgent, state: AgentRunning) =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val metadata = state.metadata.copy(lastUpdate = timestamp)
      val current = AgentRegistration(state.registration, state.metadata, lsn, committed = true)
      val commit = AgentRegistration(op.registration, metadata, lsn + 1, committed = false)
      val nextData = AgentRunning(state.agentId, op.registration, metadata)
      goto(AgentUpdating) using AgentUpdating(op, sender(), current, commit)

    /* retire all running checks */
    case Event(op: RetireAgent, state: AgentRunning) =>
      val tombstone = DateTime.now(DateTimeZone.UTC).plus(retirementPeriod.toMillis)
      val metadata = state.metadata.copy(expires = Some(tombstone))
      val current = AgentRegistration(state.registration, state.metadata, lsn, committed = true)
      val commit = AgentRegistration(state.registration, metadata, lsn + 1, committed = false)
      goto(AgentRetiring) using AgentRetiring(op, sender(), current, commit)

    /* ignore check status from top level checks */
    case Event(status: CheckStatus, state: AgentRunning) =>
      stay()

    /* forward check operations to the specified check */
    case Event(op: CheckOperation, state: AgentRunning) =>
      checks.get(op.checkRef.checkId) match {
        case Some(checkActor: CheckActor) =>
          checkActor.actor.forward(op)
        case None =>
          sender() ! CheckOperationFailed(op, ApiException(ResourceNotFound))
      }
      stay()

    /* handle notifications which have been passed up from Check */
    case Event(notification: NotificationEvent, state: AgentRunning) =>
      services ! notification
      stay()

    /* clean up retired checks, reanimate zombie checks */
    case Event(Terminated(actorRef), state: AgentRunning) =>
      val (checkRef,lsn) = retiredChecks(actorRef)
      checks = checks - checkRef
      retiredChecks.remove(actorRef)
      if (zombieChecks.contains(checkRef)) {
        zombieChecks.remove(checkRef)
        applyAgentRegistration(state.agentId, state.registration, lsn)
      } else
        log.debug("check {} has been terminated", checkRef)
      stay()
  }

  onTransition {
    /* start the agent update task when we are transitioning to UPDATING */
    case _ -> AgentUpdating =>
      val state = nextStateData.asInstanceOf[AgentUpdating]
      context.actorOf(UpdateAgentTask.props(state.op, state.sender, state.current, state.commit, services))
  }

  /*
   * UPDATING is a transitional state
   */
  when(AgentUpdating) {

    case Event(result: UpdateAgentTaskComplete, state: AgentUpdating) =>
      goto(AgentRunning) using AgentRunning(state.op.agentId, result.registration, result.metadata)

    /* stash any agent operations for later */
    case Event(op: AgentOperation, _) =>
      stash()
      stay()

    /* stash any check operations for later */
    case Event(op: CheckOperation, _) =>
      stash()
      stay()
  }

  onTransition {
    case _ -> AgentRetiring =>
      val state = nextStateData.asInstanceOf[AgentRetiring]
      val refs = checks.filterNot {
        case (_, checkActor) => retiredChecks.contains(checkActor.actor)
      }.map {
        case (checkId, checkActor) =>
          retiredChecks.put(checkActor.actor, (checkId, state.commit.lsn))
          checkActor.actor
      }.toSet
      context.actorOf(RetireAgentTask.props(state.op, state.sender, state.current, state.commit, refs, services))
  }

  /*
   * RETIRING is a transitional state from RUNNING to RETIRED.  in RETIRING state
   * we tell the registry manager to mark us as retired, then upon confirmation we
   * write a tombstone entry and retire all checks.  once the tombstone has been
   * written and all checks are terminated, we transition to RETIRED.
   */
  when(AgentRetiring) {

    /* terminate all running checks */
    case Event(result: RetireAgentTaskComplete, state: AgentRetiring) =>
      checks.foreach { case (_,checkActor) => checkActor.actor ! PoisonPill }
      stay()

    /* check actor has terminated */
    case Event(Terminated(actorRef), state: AgentRetiring) =>
      val (checkRef,_) = retiredChecks(actorRef)
      checks = checks - checkRef
      retiredChecks.remove(actorRef)
      if (zombieChecks.contains(checkRef))
        zombieChecks.remove(checkRef)
      if (checks.isEmpty) {
        goto(AgentRetired) using AgentRetired(state.op.agentId, state.commit.spec, state.commit.metadata)
      } else stay()

    /* stash any agent operations for later */
    case Event(op: AgentOperation, _) =>
      stash()
      stay()

    /* stash any check operations for later */
    case Event(op: CheckOperation, _) =>
      stash()
      stay()
  }

  onTransition {
    case _ -> AgentRetired => nextStateData match {
      case state: AgentRetired =>
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
  when(AgentRetired) {

    /* active retirement period has ended, stop agent */
    case Event(StopAgent, state: AgentRetired) =>
      stop()

    /* register a new generation of the agent */
    case Event(op: RegisterAgent, state: AgentRetired) =>
      self.forward(op)
      goto(AgentIncubating) using NoData

    case Event(op: DescribeAgent, state: AgentRetired) =>
      stay() replying DescribeAgentResult(op, state.registration, state.metadata)

    case Event(op: MatchAgent, state: AgentRetired) =>
      stay() replying AgentOperationFailed(op, ApiException(ResourceNotFound))

    /* agent doesn't exist anymore, so return resource not found */
    case Event(op: AgentOperation, _) =>
      stay() replying AgentOperationFailed(op, ApiException(ResourceNotFound))

    /* agent doesn't exist anymore, so return resource not found */
    case Event(op: CheckOperation, _) =>
      stay() replying CheckOperationFailed(op, ApiException(ResourceNotFound))
  }

  /**
   * given a registration, return the set of all check resources, including
   * the implicit containers.
   */
  def makeRegistrationSet(registration: AgentSpec): Set[CheckId] = {
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
  def applyAgentRegistration(agentId: AgentId, registration: AgentSpec, lsn: Long): Unit = {

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
  case object AgentIncubating extends State
  case object AgentRegistering extends State
  case object AgentInitializing extends State
  case object AgentRecovering extends State
  case object AgentRunning extends State
  case object AgentUpdating extends State
  case object AgentRetiring extends State
  case object AgentRetired extends State

  sealed trait Data
  case object NoData extends Data
  case class AgentInitializing(op: ReviveAgent, sender: ActorRef) extends Data
  case class AgentRegistering(op: RegisterAgent, sender: ActorRef) extends Data
  case class AgentRecovering(commit: CommitRegistration, nextState: State, nextData: Data) extends Data
  case class AgentRunning(agentId: AgentId, registration: AgentSpec, metadata: AgentMetadata) extends Data
  case class AgentUpdating(op: UpdateAgent, sender: ActorRef, current: AgentRegistration, commit: AgentRegistration) extends Data
  case class AgentRetiring(op: RetireAgent, sender: ActorRef, current: AgentRegistration, commit: AgentRegistration) extends Data
  case class AgentRetired(agentId: AgentId, registration: AgentSpec, metadata: AgentMetadata) extends Data

  case object StopAgent
}

case class ChangeCheck(checkType: String, policy: CheckPolicy, factory: ProcessorFactory, children: Set[CheckRef], lsn: Long)
case class RetireCheck(lsn: Long)

/**
 *
 */
sealed trait AgentOperation extends ServiceOperation { val agentId: AgentId }
sealed trait AgentCommand extends ServiceCommand with AgentOperation
sealed trait AgentQuery extends ServiceQuery with AgentOperation
case class AgentOperationFailed(op: AgentOperation, failure: Throwable) extends ServiceOperationFailed

case class RegisterAgent(agentId: AgentId, registration: AgentSpec) extends AgentCommand
case class RegisterAgentResult(op: RegisterAgent, metadata: AgentMetadata)

case class ReviveAgent(agentId: AgentId) extends AgentCommand
case class ReviveAgentResult(op: ReviveAgent, metadata: AgentMetadata)

case class UpdateAgent(agentId: AgentId, registration: AgentSpec) extends AgentCommand
case class UpdateAgentResult(op: UpdateAgent, metadata: AgentMetadata)

case class RetireAgent(agentId: AgentId) extends AgentCommand
case class RetireAgentResult(op: RetireAgent, metadata: AgentMetadata)

case class DeleteAgent(agentId: AgentId, generation: Long) extends AgentCommand
case class DeleteAgentResult(op: DeleteAgent)

case class DescribeAgent(agentId: AgentId) extends AgentQuery
case class DescribeAgentResult(op: DescribeAgent, registration: AgentSpec, metadata: AgentMetadata)

case class MatchAgent(agentId: AgentId, matchers: Set[CheckMatcher]) extends AgentQuery
case class MatchAgentResult(op: MatchAgent, refs: Set[CheckRef])
