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
            var probeType: String,
            var children: Set[ProbeRef],
            var policy: ProbePolicy,
            var factory: ProcessorFactory,
            val probeGeneration: Long,
            val services: ActorRef,
            val metricsBus: MetricsBus) extends LoggingFSM[Probe.State,Probe.Data] with Stash with ProcessingOps {
  import Probe._

  // config
  val commitTimeout = 5.seconds

  // state
  var processor: BehaviorProcessor = null
  var lastCommitted: Option[DateTime] = None
  val alertTimer = new Timer(context, self, ProbeAlertTimeout)
  val commitTimer = new Timer(context, self, ProbeCommitTimeout)
  val expiryTimer = new Timer(context, self, ProbeExpiryTimeout)

  /**
   * before starting the FSM, request the ProbeStatus from the state service.  the result
   * of this query determines which FSM state we transition to from Initializing.
   */
  override def preStart(): Unit = {
    // ask state service what our current status is
    val op = InitializeProbeStatus(probeRef, now())
    services ! op
    commitTimer.start(commitTimeout)
    startWith(InitializingProbe, InitializingProbe(op))
    /* perform FSM initialization at the last possible moment */
    initialize()
  }

  /*
   * wait for ProbeState from state service.  if the lsn returned equals the probe
   * generation, then transition to the appropriate behavior, otherwise transition
   * directly to Retired.
   */
  when(InitializingProbe) {

    /* retrieve the current probe status */
    case Event(query: GetProbeStatus, _) =>
      sender() ! GetProbeStatusResult(query, getProbeStatus)
      stay()

    /* ignore result if it doesn't match the in-flight request */
    case Event(result: InitializeProbeStatusResult, state: InitializingProbe) if result.op != state.command =>
      stay()

    case Event(result: InitializeProbeStatusResult, state: InitializingProbe) =>
      commitTimer.stop()
      result.status match {
        // rehydrate probe status from state service
        case Some(status) =>
          log.debug("gen {}: received initial status: {}", probeGeneration, status)
          applyStatus(status)
          lastCommitted = Some(status.timestamp)
        case None =>
          // there is no previous probe status, do nothing
      }
      // switch to retired behavior if latest probe status is retired
      if (lifecycle == ProbeRetired) {
        log.debug("gen {}: probe becomes retired", probeGeneration, probeRef)
        goto(StaleProbe) using NoData
      }
      // otherwise replay any stashed messages and transition to initialized
      else {
        val change = ChangeProbe(probeType, policy, factory, children, probeGeneration)
        enqueue(QueuedChange(change, now()))
        unstashAll()
        goto(RunningProbe) using RunningProbe()
      }

    case Event(result: StateServiceOperationFailed, state: InitializingProbe) if result.op != state.command =>
      stay()

    // FIXME: is this needed?  i don't think so.
    case Event(StateServiceOperationFailed(_, failure: ApiException), _) if failure.failure == ResourceNotFound =>
      log.debug("probe {} becomes retired", probeRef)
      commitTimer.stop()
      goto(StaleProbe) using NoData

    case Event(StateServiceOperationFailed(op, failure), _) =>
      log.debug("gen {}: failure receiving initial state: {}", probeGeneration, failure)
      commitTimer.stop()
      throw failure

    /* timed out waiting for initialization from state service */
    case Event(ProbeCommitTimeout, _) =>
      log.debug("gen {}: timeout while receiving initial state", probeGeneration)
      val op = InitializeProbeStatus(probeRef, now())
      services ! op
      commitTimer.restart(commitTimeout)
      stay() using InitializingProbe(op)

    /* tell client to retry probe operations */
    case Event(op: ProbeOperation, _) =>
      stay() replying ProbeOperationFailed(op, ApiException(RetryLater))

    /* stash any other messages for processing later */
    case Event(other, _) =>
      stash()
      stay()
  }

  /*
   * probe is ready to process messages.
   */
  when(RunningProbe) {

    /* retrieve the current probe status */
    case Event(query: GetProbeStatus, _) =>
      stay() replying GetProbeStatusResult(query, getProbeStatus)

    /* process a probe evaluation from the client */
    case Event(command: ProcessProbeEvaluation, state: RunningProbe) =>
      enqueue(QueuedCommand(command, sender()))
      stay()

    /* if the probe behavior has changed, then transition to a new state */
    case Event(change: ChangeProbe, state: RunningProbe) =>
      enqueue(QueuedChange(change, now()))
      stay()

    /* if the probe behavior has retired, then update our state */
    case Event(retire: RetireProbe, state: RunningProbe) =>
      enqueue(QueuedRetire(retire, now()))
      goto(RetiringProbe)

    /* process child status and update state */
    case Event(event: ChildMutates, state: RunningProbe) =>
      if (children.contains(event.probeRef)) { enqueue(QueuedEvent(event, now())) }
      stay()

    /* process alert timeout and update state */
    case Event(ProbeAlertTimeout, state: RunningProbe) =>
      enqueue(QueuedEvent(ProbeAlertTimeout, now()))
      stay()

    /* process expiry timeout and update state */
    case Event(ProbeExpiryTimeout, state: RunningProbe) =>
      enqueue(QueuedEvent(ProbeExpiryTimeout, now()))
      stay()

    /* process probe commands */
    case Event(command: ProbeCommand, state: RunningProbe) =>
      enqueue(QueuedCommand(command, sender()))
      stay()

    /* probe state has been committed, now we can apply the mutation */
    case Event(result: UpdateProbeStatusResult, state: RunningProbe) =>
      commit()
      stay()

    /* state failed to commit */
    case Event(failure: StateServiceOperationFailed, state: RunningProbe) =>
      recover()
      stay()

    /* timeout waiting to commit */
    case Event(ProbeCommitTimeout, state: RunningProbe) =>
      recover()
      stay()
  }

  /*
   * probe is transitioning from running to retired.  once the probe state is deleted
   * from the state service, the probe is stopped.
   */
  when(RetiringProbe) {

    /* retrieve the current probe status */
    case Event(query: GetProbeStatus, _) =>
      sender() ! GetProbeStatusResult(query, getProbeStatus)
      stay()

    /* probe state has been committed, now we can apply the mutation */
    case Event(result: UpdateProbeStatusResult, state: RunningProbe) =>
      commit()
      stay()

    /* probe delete has completed, stop the actor */
    case Event(result: DeleteProbeStatusResult, state: RunningProbe) =>
      commit()
      stop()

    /* state failed to commit */
    case Event(failure: StateServiceOperationFailed, state: RunningProbe) =>
      recover()
      stay()

    /* timeout waiting to commit */
    case Event(ProbeCommitTimeout, state: RunningProbe) =>
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
}

object Probe {
  def props(probeRef: ProbeRef,
            parent: ActorRef,
            probeType: String,
            children: Set[ProbeRef],
            policy: ProbePolicy,
            factory: ProcessorFactory,
            probeGeneration: Long,
            services: ActorRef,
            metricsBus: MetricsBus) = {
    Props(classOf[Probe], probeRef, parent, probeType, children, policy, factory, probeGeneration, services, metricsBus)
  }


  sealed trait State
  case object InitializingProbe extends State
  case object ChangingProbe extends State
  case object RunningProbe extends State
  case object RetiringProbe extends State
  case object StaleProbe extends State

  sealed trait Data
  case class InitializingProbe(command: InitializeProbeStatus) extends Data
  case class RunningProbe() extends Data
  case class ChangingProbe(command: ChangeProbe) extends Data
  case class RetiringProbe(lsn: Long) extends Data
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
