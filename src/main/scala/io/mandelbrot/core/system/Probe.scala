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
import scala.concurrent.duration._
import java.util.UUID

import io.mandelbrot.core._
import io.mandelbrot.core.model._
import io.mandelbrot.core.registry._
import io.mandelbrot.core.state._
import io.mandelbrot.core.metrics.MetricsBus
import io.mandelbrot.core.util.Timer

import scala.util.{Failure, Success}

/**
 * the Probe actor encapsulates all of the monitoring business logic.  For every probe
 * declared by an agent or proxy, there is a corresponding Probe actor.
 */
class Probe(val probeRef: ProbeRef,
            val parent: ActorRef,
            val services: ActorRef,
            val metricsBus: MetricsBus) extends LoggingFSM[Probe.State,Probe.Data] with Stash with ProcessingOps {
  import Probe._

  // config
  val commitTimeout = 5.seconds

  // state
  var probeType: String = null
  var processor: BehaviorProcessor = null
  var factory: ProcessorFactory = null
  var policy: ProbePolicy = null
  var children: Set[ProbeRef] = null
  var probeGeneration: Long = 0L
  var lastCommitted: Option[DateTime] = None
  val alertTimer = new Timer(context, self, ProbeAlertTimeout)
  val commitTimer = new Timer(context, self, ProbeCommitTimeout)
  val expiryTimer = new Timer(context, self, ProbeExpiryTimeout)

  startWith(Incubating, NoData)

  /*
   *
   */
  when(Incubating) {

    /* initialize the probe using parameters from the proposed processor */
    case Event(change: ChangeProbe, NoData) =>
      val proposed = change.factory.implement()
      val params = proposed.initialize()
      val op = InitializeProbeStatus(probeRef, now())
      goto(Initializing) using Initializing(change, proposed, op)

    /* stash any other messages for processing later */
    case Event(_, NoData) =>
      stash()
      stay()
  }

  onTransition {
    case _ -> Initializing =>
      nextStateData match {
        case state: Initializing => services ! state.inflight
        case _ =>
      }
      commitTimer.restart(commitTimeout)
  }

  /*
   *
   */
  when(Initializing) {

    /* ignore result if it doesn't match the in-flight request */
    case Event(result: InitializeProbeStatusResult, state: Initializing) if result.op != state.inflight =>
      stay()

    /* ignore failure if it doesn't match the in-flight request */
    case Event(result: StateServiceOperationFailed, state: Initializing) if result.op != state.inflight =>
      stay()

    /* configure processor using initial state */
    case Event(result: InitializeProbeStatusResult, state: Initializing) =>
      commitTimer.stop()
      val effect = state.proposed.configure(result.status.getOrElse(getProbeStatus), state.change.children)
      lastCommitted = result.status.map(_.timestamp)
      val op = UpdateProbeStatus(probeRef, effect.status, effect.notifications, lastCommitted)
      goto(Configuring) using Configuring(state.change, state.proposed, op)

    /* timed out waiting for initialization from state service */
    case Event(ProbeCommitTimeout, state: Initializing) =>
      log.debug("gen {}: timeout while receiving initial state", probeGeneration)
      services ! state.inflight
      commitTimer.restart(commitTimeout)
      stay()

    /* received an unhandled exception, so bail out */
    case Event(StateServiceOperationFailed(op, failure), state: Initializing) =>
      log.debug("gen {}: failure receiving initial state: {}", probeGeneration, failure)
      commitTimer.stop()
      throw failure

    /* stash any other messages for processing later */
    case Event(other, state: Initializing) =>
      stash()
      stay()
  }

  onTransition {
    case _ -> Configuring =>
      nextStateData match {
        case state: Configuring => services ! state.inflight
        case _ =>
      }
      commitTimer.restart(commitTimeout)
  }

  /*
   *
   */
  when(Configuring) {

    /* ignore result if it doesn't match the in-flight request */
    case Event(result: UpdateProbeStatusResult, state: Configuring) if result.op != state.inflight =>
      stay()

    /* ignore failure if it doesn't match the in-flight request */
    case Event(result: StateServiceOperationFailed, state: Configuring) if result.op != state.inflight =>
      stay()

    /* apply status processor using initial state */
    case Event(result: UpdateProbeStatusResult, state: Configuring) =>
      commitTimer.stop()
      val status = state.inflight.status
      applyStatus(status)
      processor = state.proposed
      policy = state.change.policy
      children = state.change.children
      parent ! ChildMutates(probeRef, status)
      notify(state.inflight.notifications)
      lastCommitted = Some(status.timestamp)
      goto(Running) using NoData

    /* timed out waiting for initialization from state service */
    case Event(ProbeCommitTimeout, state: Configuring) =>
      log.debug("gen {}: timeout while updating configured state", probeGeneration)
      services ! state.inflight
      commitTimer.restart(commitTimeout)
      stay()

    /* received an unhandled exception, so bail out */
    case Event(StateServiceOperationFailed(op, failure), state: Configuring) =>
      log.debug("gen {}: failure updating configured state: {}", probeGeneration, failure)
      commitTimer.stop()
      throw failure

    /* stash any other messages for processing later */
    case Event(_, state: Configuring) =>
      stash()
      stay()
  }

  onTransition {
    case Configuring -> Running => unstashAll()
  }

  /*
   * probe is ready to process messages.
   */
  when(Running) {

    /* retrieve the current probe status */
    case Event(query: GetProbeStatus, NoData) =>
      stay() replying GetProbeStatusResult(query, getProbeStatus)

    /* process a probe evaluation from the client */
    case Event(command: ProcessProbeEvaluation, NoData) =>
      enqueue(QueuedCommand(command, sender()))
      stay()

    /* if the probe behavior has changed, then transition to a new state */
    case Event(change: ChangeProbe, NoData) =>
      // if queue is empty, put the message back in the mailbox to reprocess in Incubating state
      if (idle) {
        self ! change
        goto(Incubating) using NoData
      }
      // otherwise hold onto the change and drain the queue first
      else {
        goto(Changing) using Changing(change)
      }

    /* if the probe behavior has retired, then update our state */
    case Event(retire: RetireProbe, NoData) =>
      enqueue(QueuedRetire(retire, now()))
      goto(Retiring)

    /* process child status and update state */
    case Event(event: ChildMutates, NoData) =>
      if (children.contains(event.probeRef)) { enqueue(QueuedEvent(event, now())) }
      stay()

    /* process alert timeout and update state */
    case Event(ProbeAlertTimeout, NoData) =>
      enqueue(QueuedEvent(ProbeAlertTimeout, now()))
      stay()

    /* process expiry timeout and update state */
    case Event(ProbeExpiryTimeout, NoData) =>
      enqueue(QueuedEvent(ProbeExpiryTimeout, now()))
      stay()

    /* process probe commands */
    case Event(command: ProbeCommand, NoData) =>
      enqueue(QueuedCommand(command, sender()))
      stay()

    /* probe state has been committed, now we can apply the mutation */
    case Event(result: UpdateProbeStatusResult, NoData) =>
      commit()
      stay()

    /* state failed to commit */
    case Event(failure: StateServiceOperationFailed, NoData) =>
      recover()
      stay()

    /* timeout waiting to commit */
    case Event(ProbeCommitTimeout, NoData) =>
      recover()
      stay()
  }

  /*
   *
   */
  when(Changing) {

    /* retrieve the current probe status */
    case Event(query: GetProbeStatus, state: Changing) =>
      sender() ! GetProbeStatusResult(query, getProbeStatus)
      stay()

    /* probe state has been committed, now we can apply the mutation */
    case Event(result: UpdateProbeStatusResult, state: Changing) =>
      commit()
      if (idle) goto(Incubating) using NoData else stay()

    /* state failed to commit */
    case Event(failure: StateServiceOperationFailed, state: Changing) =>
      recover()
      if (idle) goto(Incubating) using NoData else stay()

    /* timeout waiting to commit */
    case Event(ProbeCommitTimeout, state: Changing) =>
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
   * probe is transitioning from running to retired.  once the probe state is deleted
   * from the state service, the probe is stopped.
   */
  when(Retiring) {

    /* retrieve the current probe status */
    case Event(query: GetProbeStatus, _) =>
      sender() ! GetProbeStatusResult(query, getProbeStatus)
      stay()

    /* probe state has been committed, now we can apply the mutation */
    case Event(result: UpdateProbeStatusResult, NoData) =>
      commit()
      stay()

    /* probe delete has completed, stop the actor */
    case Event(result: DeleteProbeStatusResult, NoData) =>
      commit()
      stop()

    /* state failed to commit */
    case Event(failure: StateServiceOperationFailed, NoData) =>
      recover()
      stay()

    /* timeout waiting to commit */
    case Event(ProbeCommitTimeout, NoData) =>
      recover()
      stay()

    /* drop any other messages */
    case Event(other, _) =>
      stay()
  }

  /**
   * ensure all timers are stopped, so we don't get spurious messages (and the corresponding
   * log messages in the debug log).
   */
  override def postStop(): Unit = {
    alertTimer.stop()
    commitTimer.stop()
    expiryTimer.stop()
  }

  initialize()
}

object Probe {
  def props(probeRef: ProbeRef,
            parent: ActorRef,
            services: ActorRef,
            metricsBus: MetricsBus) = {
    Props(classOf[Probe], probeRef, parent, services, metricsBus)
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
  case class Initializing(change: ChangeProbe, proposed: BehaviorProcessor, inflight: InitializeProbeStatus) extends Data
  case class Configuring(change: ChangeProbe, proposed: BehaviorProcessor, inflight: UpdateProbeStatus) extends Data
  case class Changing(pending: ChangeProbe) extends Data
  case class Retiring(lsn: Long) extends Data
  case class Retired(lsn: Long) extends Data
  case object NoData extends Data

}

sealed trait ProbeEvent
case class ChildMutates(probeRef: ProbeRef, status: ProbeStatus) extends ProbeEvent
case object ProbeCommitTimeout extends ProbeEvent
case object ProbeAlertTimeout extends ProbeEvent
case object ProbeExpiryTimeout extends ProbeEvent

/* */
trait ProbeOperation extends ServiceOperation { val probeRef: ProbeRef }
sealed trait ProbeCommand extends ServiceCommand with ProbeOperation
sealed trait ProbeQuery extends ServiceQuery with ProbeOperation
sealed trait ProbeResult
case class ProbeOperationFailed(op: ProbeOperation, failure: Throwable) extends ServiceOperationFailed

case class GetProbeStatus(probeRef: ProbeRef) extends ProbeQuery
case class GetProbeStatusResult(op: GetProbeStatus, status: ProbeStatus) extends ProbeResult

case class GetProbeCondition(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime], limit: Int, last: Option[String]) extends ProbeQuery
case class GetProbeConditionResult(op: GetProbeCondition, page: ProbeConditionPage) extends ProbeResult

case class GetProbeNotifications(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime], limit: Int, last: Option[String]) extends ProbeQuery
case class GetProbeNotificationsResult(op: GetProbeNotifications, page: ProbeNotificationsPage) extends ProbeResult

case class GetProbeMetrics(probeRef: ProbeRef, from: Option[DateTime], to: Option[DateTime], limit: Int, last: Option[String]) extends ProbeQuery
case class GetProbeMetricsResult(op: GetProbeMetrics, page: ProbeMetricsPage) extends ProbeResult

case class ProcessProbeEvaluation(probeRef: ProbeRef, evaluation: ProbeEvaluation) extends ProbeCommand
case class ProcessProbeEvaluationResult(op: ProcessProbeEvaluation) extends ProbeResult

case class SetProbeSquelch(probeRef: ProbeRef, squelch: Boolean) extends ProbeCommand
case class SetProbeSquelchResult(op: SetProbeSquelch, condition: ProbeCondition) extends ProbeResult

case class AcknowledgeProbe(probeRef: ProbeRef, correlationId: UUID) extends ProbeCommand
case class AcknowledgeProbeResult(op: AcknowledgeProbe, condition: ProbeCondition) extends ProbeResult

case class UnacknowledgeProbe(probeRef: ProbeRef, acknowledgementId: UUID) extends ProbeCommand
case class UnacknowledgeProbeResult(op: UnacknowledgeProbe, condition: ProbeCondition) extends ProbeResult
