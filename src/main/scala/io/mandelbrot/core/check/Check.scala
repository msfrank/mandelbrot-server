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

package io.mandelbrot.core.check

import akka.actor._
import akka.pattern.ask
import akka.pattern.pipe
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.duration._
import java.util.UUID

import io.mandelbrot.core._
import io.mandelbrot.core.model._
import io.mandelbrot.core.state._
import io.mandelbrot.core.agent.{ObservationBus, RetireCheck}
import io.mandelbrot.core.util.Timer

/**
 * the Check actor encapsulates all of the monitoring business logic.  For every check
 * declared by an agent or proxy, there is a corresponding Check actor.
 */
class Check(val checkRef: CheckRef,
            val generation: Long,
            val parent: ActorRef,
            val services: ActorRef,
            val observationBus: ObservationBus) extends LoggingFSM[Check.State,Check.Data] with Stash with ProcessingOps {
  import Check._
  import context.dispatcher

  // config
  val commitTimeout = 5.seconds
  val queryTimeout = 5.seconds

  // state
  var checkType: String = null
  var processor: BehaviorProcessor = null
  var factory: ProcessorFactory = null
  var policy: CheckPolicy = null
  var children: Set[CheckRef] = null
  var lastCommitted: Option[DateTime] = None
  val commitTimer = new Timer(context, self, CheckCommitTimeout)
  val expiryTimer = new Timer(context, self, CheckExpiryTimeout)
  val alertTimer = new Timer(context, self, CheckAlertTimeout)

  startWith(Incubating, NoData)

  /*
   * The Check always starts in INCUBATING state.  it waits for the first message, which is
   * always ChangeCheck.  ChangeCheck contains all registration data needed to configure the
   * check for the first time.
   */
  when(Incubating) {

    /* initialize the check using parameters from the proposed processor */
    case Event(change: ChangeCheck, NoData) =>
      val proposed = change.factory.implement()
      val initializers = proposed.initialize(checkRef, generation).initializers
      context.actorOf(InitializeCheckTask.props(checkRef, generation, initializers, self, services))
      goto(Initializing) using Initializing(change, proposed)

    /* stash any other messages for processing later */
    case Event(_, NoData) =>
      stash()
      stay()
  }

  onTransition {
    case _ -> Initializing =>
      commitTimer.restart(commitTimeout)
  }

  /*
   * INITIALIZING is a transitional state.  in INCUBATING state we notified our configurator
   * of the state we require (the initializers); now we wait for the configurator to perform
   * the queries on our behalf and give us the results in the InitializeCheck message.  once
   * we receive the InitializeCheck message, we transition to Configuring.  If we time out
   * while waiting for the message, or we receive a failure message, then we throw an exception.
   */
  when(Initializing) {

    /* configure processor using initial state */
    case Event(initialize: InitializeCheckTaskComplete, state: Initializing) =>
      commitTimer.stop()
      val effect = state.proposed.configure(checkRef, generation, initialize.status,
        initialize.observations, state.change.children)
      val op = UpdateStatus(checkRef, effect.status, effect.notifications, commitEpoch = true)
      goto(Configuring) using Configuring(state.change, state.proposed, op)

    /* timed out waiting for initialization from state service */
    case Event(CheckCommitTimeout, state: Initializing) =>
      throw new Exception("timeout while receiving initial state")

    /* initialization failed, let supervision handle it */
    case Event(InitializeCheckTaskFailed(ex), state: Initializing) =>
      throw ex

    /* stash any other messages for processing later */
    case Event(_, state: Initializing) =>
      stash()
      stay()
  }

  onTransition {
    case _ -> Configuring =>
      val state = nextStateData.asInstanceOf[Configuring]
      services ! state.inflight
      commitTimer.restart(commitTimeout)
  }

  /*
   *
   */
  when(Configuring) {

    /* ignore result if it doesn't match the in-flight request */
    case Event(result: UpdateStatusResult, state: Configuring) if result.op != state.inflight =>
      stay()

    /* ignore failure if it doesn't match the in-flight request */
    case Event(result: StateServiceOperationFailed, state: Configuring) if result.op != state.inflight =>
      stay()

    /* apply status processor using initial state */
    case Event(result: UpdateStatusResult, state: Configuring) =>
      commitTimer.stop()
      val status = state.inflight.status
      processor = state.proposed
      policy = state.change.policy
      children = state.change.children
      updateStatus(status)
      parent ! ChildMutates(checkRef, status)
      notify(state.inflight.notifications)
      lastCommitted = Some(status.timestamp)
      goto(Running) using NoData

    /* timed out waiting for initialization from state service */
    case Event(CheckCommitTimeout, state: Configuring) =>
      log.debug("timeout while updating configured state")
      services ! state.inflight
      commitTimer.restart(commitTimeout)
      stay()

    /* received an unhandled exception, so bail out */
    case Event(StateServiceOperationFailed(op, failure), state: Configuring) =>
      log.debug("failure updating configured state: {}", failure)
      commitTimer.stop()
      throw failure

    /* stash any other messages for processing later */
    case Event(_, state: Configuring) =>
      stash()
      stay()
  }

  onTransition {
    case Configuring -> Running =>
      val state = stateData.asInstanceOf[Configuring]
      unstashAll()
  }

  /*
   * check is ready to process messages.
   */
  when(Running) {

    /* retrieve the current check status */
    case Event(query: GetCheckStatus, NoData) =>
      stay() replying GetCheckStatusResult(query, getCheckStatus)

    /* query state service for condition history */
    case Event(query: GetCheckCondition, NoData) =>
      services.ask(GetConditionHistory(checkRef, generation, query.from, query.to, query.limit,
        query.fromInclusive, query.toExclusive, query.descending, query.last))(queryTimeout).map {
        case result: GetConditionHistoryResult =>
          GetCheckConditionResult(query, result.page)
        case failure: StateServiceOperationFailed =>
          CheckOperationFailed(query, failure.failure)
      }.pipeTo(sender())
      stay()

    /* */
    case Event(query: GetCheckNotifications, NoData) =>
      services.ask(GetNotificationsHistory(checkRef, generation, query.from, query.to, query.limit,
        query.fromInclusive, query.toExclusive, query.descending, query.last))(queryTimeout).map {
        case result: GetNotificationsHistoryResult =>
          GetCheckNotificationsResult(query, result.page)
        case failure: StateServiceOperationFailed =>
          CheckOperationFailed(query, failure.failure)
      }.pipeTo(sender())
      stay()

    /* process a check evaluation from the client */
    case Event(command: ProcessObservation, NoData) =>
      enqueue(QueuedObservation(command.probeId, command.observation))
      stay()

    /* if the check behavior has changed, then transition to a new state */
    case Event(change: ChangeCheck, NoData) =>
      // if queue is empty, put the message back in the mailbox to reprocess in Incubating state
      if (idle) {
        self ! change
        goto(Incubating) using NoData
      }
      // otherwise hold onto the change and drain the queue first
      else {
        goto(Changing) using Changing(change)
      }

    /* if the check behavior has retired, then update our state */
    case Event(retire: RetireCheck, NoData) =>
      enqueue(QueuedRetire(retire, now()))
      goto(Retiring)

    /* process child status and update state */
    case Event(event: ChildMutates, NoData) =>
      if (children.contains(event.checkRef)) { enqueue(QueuedEvent(event, now())) }
      stay()

    /* process alert timeout and update state */
    case Event(CheckAlertTimeout, NoData) =>
      enqueue(QueuedEvent(CheckAlertTimeout, now()))
      stay()

    /* process expiry timeout and update state */
    case Event(CheckExpiryTimeout, NoData) =>
      enqueue(QueuedEvent(CheckExpiryTimeout, now()))
      stay()

    /* process check commands */
    case Event(command: CheckCommand, NoData) =>
      enqueue(QueuedCommand(command, sender()))
      stay()

    /* check state has been committed, now we can apply the mutation */
    case Event(result: UpdateStatusResult, NoData) =>
      commit()
      stay()

    /* state failed to commit */
    case Event(failure: StateServiceOperationFailed, NoData) =>
      recover()
      stay()

    /* timeout waiting to commit */
    case Event(CheckCommitTimeout, NoData) =>
      recover()
      stay()
  }

  /*
   *
   */
  when(Changing) {

    /* retrieve the current check status */
    case Event(query: GetCheckStatus, state: Changing) =>
      sender() ! GetCheckStatusResult(query, getCheckStatus)
      stay()

    /* check state has been committed, now we can apply the mutation */
    case Event(result: UpdateStatusResult, state: Changing) =>
      commit()
      if (idle) goto(Incubating) using NoData else stay()

    /* state failed to commit */
    case Event(failure: StateServiceOperationFailed, state: Changing) =>
      recover()
      if (idle) goto(Incubating) using NoData else stay()

    /* timeout waiting to commit */
    case Event(CheckCommitTimeout, state: Changing) =>
      recover()
      if (idle) goto(Incubating) using NoData else stay()

    /* drop any other messages */
    case Event(_, state: Changing) =>
      stash()
      stay()
  }

  onTransition {
    case Changing -> Incubating =>
      stateData match {
        case state: Changing => self ! state.pending
        case _ =>
      }
  }
  /*
   * check is transitioning from running to retired.  once the check state is deleted
   * from the state service, the check is stopped.
   */
  when(Retiring) {

    /* retrieve the current check status */
    case Event(query: GetCheckStatus, _) =>
      sender() ! GetCheckStatusResult(query, getCheckStatus)
      stay()

    /* check state has been committed, stop the actor */
    case Event(result: UpdateStatusResult, NoData) =>
      commit()
      log.debug("{} is retired", result.op.checkRef)
      stay()

    /* state failed to commit */
    case Event(failure: StateServiceOperationFailed, NoData) =>
      recover()
      stay()

    /* timeout waiting to commit */
    case Event(CheckCommitTimeout, NoData) =>
      recover()
      stay()

    /* drop any other messages */
    case Event(other, _) =>
      stay()
  }

  /**
   * ensure all timers are stopped, so we don't get spurious messages (and the corresponding
   * log messages in the debug log), and unsubscribe completely from the observation bus.
   */
  override def postStop(): Unit = {
    // stop any timers which might still be running
    alertTimer.stop()
    commitTimer.stop()
    expiryTimer.stop()
  }

  initialize()
}

object Check {
  def props(checkRef: CheckRef,
            generation: Long,
            parent: ActorRef,
            services: ActorRef,
            metricsBus: ObservationBus) = {
    Props(classOf[Check], checkRef, generation, parent, services, metricsBus)
  }


  sealed trait State
  case object Incubating extends State
  case object Initializing extends State
  case object Configuring extends State
  case object Changing extends State
  case object Running extends State
  case object Retiring extends State
  case object Retired extends State

  sealed trait Data
  case class Initializing(change: ChangeCheck, proposed: BehaviorProcessor) extends Data
  case class Configuring(change: ChangeCheck, proposed: BehaviorProcessor, inflight: UpdateStatus) extends Data
  case class Changing(pending: ChangeCheck) extends Data
  case class Retiring(lsn: Long) extends Data
  case class Retired(lsn: Long) extends Data
  case object NoData extends Data

}

/* */
case class ChangeCheck(checkType: String, policy: CheckPolicy, factory: ProcessorFactory, children: Set[CheckRef], lsn: Long)
case class ProcessObservation(probeId: ProbeId, observation: Observation)

/* */
sealed trait CheckEvent
case class ChildMutates(checkRef: CheckRef, status: CheckStatus) extends CheckEvent
case object CheckCommitTimeout extends CheckEvent
case object CheckAlertTimeout extends CheckEvent
case object CheckExpiryTimeout extends CheckEvent

/* */
sealed trait CheckOperation extends ServiceOperation { val checkRef: CheckRef }
sealed trait CheckCommand extends ServiceCommand with CheckOperation
sealed trait CheckQuery extends ServiceQuery with CheckOperation
sealed trait CheckResult
case class CheckOperationFailed(op: CheckOperation, failure: Throwable) extends ServiceOperationFailed

case class GetCheckStatus(checkRef: CheckRef) extends CheckQuery
case class GetCheckStatusResult(op: GetCheckStatus, status: CheckStatus) extends CheckResult

case class GetCheckCondition(checkRef: CheckRef,
                             from: Option[DateTime],
                             to: Option[DateTime],
                             limit: Int,
                             fromInclusive: Boolean,
                             toExclusive: Boolean,
                             descending: Boolean,
                             last: Option[String]) extends CheckQuery
case class GetCheckConditionResult(op: GetCheckCondition, page: CheckConditionPage) extends CheckResult

case class GetCheckNotifications(checkRef: CheckRef,
                                 from: Option[DateTime],
                                 to: Option[DateTime],
                                 limit: Int,
                                 fromInclusive: Boolean,
                                 toExclusive: Boolean,
                                 descending: Boolean,
                                 last: Option[String]) extends CheckQuery
case class GetCheckNotificationsResult(op: GetCheckNotifications, page: CheckNotificationsPage) extends CheckResult

case class SetCheckSquelch(checkRef: CheckRef, squelch: Boolean) extends CheckCommand
case class SetCheckSquelchResult(op: SetCheckSquelch, condition: CheckCondition) extends CheckResult

case class AcknowledgeCheck(checkRef: CheckRef, correlationId: UUID) extends CheckCommand
case class AcknowledgeCheckResult(op: AcknowledgeCheck, condition: CheckCondition) extends CheckResult

case class UnacknowledgeCheck(checkRef: CheckRef, acknowledgementId: UUID) extends CheckCommand
case class UnacknowledgeCheckResult(op: UnacknowledgeCheck, condition: CheckCondition) extends CheckResult
