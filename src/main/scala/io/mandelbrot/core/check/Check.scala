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
import org.joda.time.DateTime
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
            val services: ActorRef) extends LoggingFSM[Check.State,Check.Data] with Stash with ProcessingOps {
  import Check._
  import context.dispatcher

  // config
  val commitTimeout = 5.seconds
  val queryTimeout = 5.seconds

  // state
  var checkType: String = null
  var processor: ActorRef = ActorRef.noSender
  var lsn: Long = 0
  var policy: CheckPolicy = null
  var children: Set[CheckRef] = null
  var lastCommitted: Option[DateTime] = None
  val commitTimer = new Timer(context, self, CheckCommitTimeout)

  startWith(Incubating, NoData)

  /*
   * The Check always starts in INCUBATING state.  it waits for the first message, which is
   * always ChangeCheck.  ChangeCheck contains all registration data needed to configure the
   * check for the first time.
   */
  when(Incubating) {

    /* initialize the check using parameters from the proposed processor */
    case Event(change: ChangeCheck, NoData) =>
      log.debug("changing check {}", checkRef)
      val op = GetStatus(checkRef, generation)
      services ! op
      goto(Initializing) using Initializing(change, op)

    /* stash any other messages for processing later */
    case Event(other, NoData) =>
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
    case Event(result: GetStatusResult, state: Initializing) =>
      commitTimer.stop()
      log.debug("received initial status: {}", result.status)
      policy = state.change.policy
      children = state.change.children
      // if check has no previous status, then generate the first status
      val status = result.status.getOrElse {
        val timestamp = Timestamp().toDateTime
        CheckStatus(generation, timestamp, CheckInitializing, None, CheckUnknown,
          Some(timestamp), Some(timestamp), None, None, squelched = false)
      }
      applyStatus(status)
      lastCommitted = Some(status.timestamp)
      // create the check processor
      processor = context.actorOf(state.change.props)
      context.watch(processor)
      // start the processor
      lsn = lsn + 1
      processor ! ChangeProcessor(lsn, services, children)
      goto(Running) using NoData

    /* timed out waiting for initialization from state service */
    case Event(CheckCommitTimeout, state: Initializing) =>
      throw new Exception("timeout while initializing")

    /* initialization failed, let supervision handle it */
    case Event(failure: StateServiceOperationFailed, state: Initializing) =>
      throw failure.failure

    /* stash any other messages for processing later */
    case Event(other, state: Initializing) =>
      stash()
      stay()
  }

  onTransition {
    case Initializing -> Running =>
      unstashAll()
  }

  /*
   * check is ready to process messages.
   */
  when(Running) {

    /* queue the processor status to be persisted */
    case Event(status: ProcessorStatus, NoData) =>
      enqueue(QueuedStatus(status, now()))
      stay()

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

    /* process check commands */
    case Event(command: CheckCommand, NoData) =>
      enqueue(QueuedCommand(command, sender()))
      stay()

    /* if the check behavior has changed, then transition to a new state */
    case Event(change: ChangeCheck, NoData) =>
      // if queue is empty, put the message back in the mailbox to
      // reprocess in Incubating state, otherwise hold onto the change
      // and drain the queue first
      if (idle) {
        val op = GetStatus(checkRef, generation)
        services ! op
        goto(Initializing) using Initializing(change, op)
      } else goto(Changing) using Changing(change)

    /* if the check behavior has retired, then update our state */
    case Event(retire: RetireCheck, NoData) =>
      enqueue(QueuedRetire(retire, now()))
      goto(Retiring)

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
    commitTimer.stop()
  }

  initialize()
}

object Check {
  def props(checkRef: CheckRef,
            generation: Long,
            parent: ActorRef,
            services: ActorRef) = {
    Props(classOf[Check], checkRef, generation, parent, services)
  }


  sealed trait State
  case object Incubating extends State
  case object Initializing extends State
  case object Changing extends State
  case object Running extends State
  case object Retiring extends State
  case object Retired extends State

  sealed trait Data
  case class Initializing(change: ChangeCheck, inflight: GetStatus) extends Data
  case class Changing(pending: ChangeCheck) extends Data
  case class Retiring(lsn: Long) extends Data
  case class Retired(lsn: Long) extends Data
  case object NoData extends Data

}

/* */
case class ChangeCheck(checkType: String, policy: CheckPolicy, props: Props, children: Set[CheckRef], lsn: Long)
case class ProcessObservation(probeId: ProbeId, observation: Observation)

/* */
sealed trait CheckEvent
case class ChildMutates(checkRef: CheckRef, status: CheckStatus) extends CheckEvent
case object CheckCommitTimeout extends CheckEvent
case object NextTick extends CheckEvent

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
