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
import akka.pattern.ask
import akka.pattern.pipe
import io.mandelbrot.core.state.{DeleteProbeState, ProbeStatusCommitted, ProbeState, InitializeProbeState}
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.duration._
import java.util.UUID

import io.mandelbrot.core.registry._
import io.mandelbrot.core.notification._
import io.mandelbrot.core.{ApiException, ResourceNotFound, ServiceMap}
import io.mandelbrot.core.metrics.MetricsBus
import io.mandelbrot.core.util.Timer

import scala.util.{Failure, Success}

/**
 * the Probe actor encapsulates all of the monitoring business logic.  For every probe
 * declared by an agent or proxy, there is a corresponding Probe actor.
 */
class Probe(val probeRef: ProbeRef,
            val parent: ActorRef,
            var children: Set[ProbeRef],
            var policy: ProbePolicy,
            var behavior: ProbeBehavior,
            val probeGeneration: Long,
            val services: ServiceMap,
            val metricsBus: MetricsBus) extends LoggingFSM[ProbeFSMState,ProbeFSMData] with Stash with ProbeOperations {
  import Probe._
  import context.dispatcher

  // config
  implicit val timeout: akka.util.Timeout = 5.seconds

  // state
  var processor: ProbeBehaviorInterface = behavior.makeProbeBehavior()
  var _lifecycle: ProbeLifecycle = ProbeInitializing
  var _health: ProbeHealth = ProbeUnknown
  var _summary: Option[String] = None
  var _lastChange: Option[DateTime] = None
  var _lastUpdate: Option[DateTime] = None
  var _correlationId: Option[UUID] = None
  var _acknowledgementId: Option[UUID] = None
  var _squelch: Boolean = false

  val expiryTimer = new Timer(context, self, ProbeExpiryTimeout)
  val alertTimer = new Timer(context, self, ProbeAlertTimeout)

  /* we start in Initializing state */
  startWith(InitializingProbe, NoData)
  initialize()

  /**
   * before starting the FSM, request the ProbeState from the state service.  the result
   * of this query determines which FSM state we transition to from Initializing.
   */
  override def preStart(): Unit = {
    // ask state service what our current status is
    services.stateService.ask(InitializeProbeState(probeRef, DateTime.now(DateTimeZone.UTC), probeGeneration)).map {
      case result @ Success(state: ProbeState) =>
        log.debug("gen {}: received initial status from state service: {} (lsn {})", probeGeneration, state.status, state.lsn)
        result
      case result @ Failure(failure: Throwable) =>
        log.debug("gen {}: failure receiving initial state from state service: {}", probeGeneration, failure)
        result
    }.pipeTo(self)
  }

  /*
   * wait for ProbeState from state service.  if the lsn returned equals the probe
   * generation, then transition to the appropriate behavior, otherwise transition
   * directly to Retired.
   */
  when(InitializingProbe) {

    case Event(Success(ProbeState(status, lsn)), _) =>
      // initialize probe state
      setProbeStatus(status)
      // this generation is not current, so switch to retired behavior
      if (lsn > probeGeneration) {
        log.debug("probe {} becomes retired (lsn {})", probeRef, lsn)
        unstashAll()
        goto(StaleProbe) using NoData
      }
      // otherwise replay any stashed messages and transition to initialized
      else {
        processor.enter(this)
        unstashAll()
        goto(RunningProbe) using NoData
      }

    case Event(Failure(failure: ApiException), _) if failure.failure == ResourceNotFound =>
      log.debug("probe {} becomes retired", probeRef)
      unstashAll()
      goto(StaleProbe) using NoData

    case Event(Failure(failure: Throwable), _) =>
      throw failure

    case Event(other, _) =>
      // TODO: drop any messages of type Message?
      stash()
      stay()
  }

  private def enqueue(state: RunningProbe, message: Any) = RunningProbe(state.inflight, state.queued :+ message)

  private def process(state: RunningProbe): RunningProbe = {
    var queued = state.queued
    while (queued.nonEmpty) {
      val mutation = processor.process(this, queued.head)
      queued = queued.tail
      mutation match {
        case None => // do nothing
        case mutation @ Some(ProbeMutation(status, _)) =>
          services.stateService ! ProbeState(status, probeGeneration)
          return RunningProbe(mutation, queued)
      }
    }
    RunningProbe(None, queued)
  }

  /*
   * probe is ready to process messages.
   */
  when(RunningProbe) {

    /* if the probe behavior has changed, then transition to a new state */
    case Event(change: ChangeProbe, _) =>
      goto(ChangingProbe) using ChangingProbe(change)

    /* if the probe behavior has updated, then update our state */
    case Event(update: UpdateProbe, _) =>
      children = update.children
      policy = update.policy
      behavior = update.behavior
      processor.update(this, behavior)
      stay()

    /* process status message and update state */
    case Event(message: StatusMessage, state: RunningProbe) =>
      stay() using process(enqueue(state, message))

    /* process metrics message and update state */
    case Event(message: MetricsMessage, state: RunningProbe) =>
      stay() using process(enqueue(state, message))

    /* process child status and update state */
    case Event(message: ProbeStatus, state: RunningProbe) =>
      if (!children.contains(message.probeRef)) stay() else {
        stay() using process(enqueue(state, message))
      }

    /* process alert timeout and update state */
    case Event(message @ ProbeAlertTimeout, state: RunningProbe) =>
      stay() using process(enqueue(state, message))

    /* process expiry timeout and update state */
    case Event(message @ ProbeExpiryTimeout, state: RunningProbe) =>
      stay() using process(enqueue(state, message))

    /* state has been committed, now we can apply it locally */
    case Event(Success(_: ProbeStatusCommitted), state @ RunningProbe(Some(mutation), _)) =>
      setProbeStatus(mutation.status)
      parent ! mutation.status
      sendNotifications(mutation.notifications)
      stay() using process(state)

    /* state failed to commit */
    case Event(Failure(ex: Throwable), state: RunningProbe) =>
      log.warning("failed to commit status for {}: {}", probeRef, ex)
      // TODO: add some retry logic here?
      stay() using process(state)

    case Event(query: GetProbeStatus, _) =>
      sender() ! GetProbeStatusResult(query, getProbeStatus)
      stay()

//    case Event(command: AcknowledgeProbe, _) =>
//      acknowledgeProbe(command, sender())
//      stay()
//
//    case Event(command: AppendProbeWorknote, _) =>
//      appendComment(command, sender())
//      stay()
//
//    case Event(command: UnacknowledgeProbe, _) =>
//      unacknowledgeProbe(command, sender())
//      stay()
//
//    case Event(command: SetProbeSquelch, _) =>
//      squelchProbe(command, sender())
//      stay()


    case Event(RetireProbe(lsn), _) =>
      goto(RetiringProbe) using RetiringProbe(lsn)
  }

  /*
   * probe is transitioning between behaviors.  we change probe state to Initializing and
   * commit it to the state service.  once the change has been committed, we transition to
   * the new behavior.
   */
  when(ChangingProbe) {

    case Event(Success(ProbeStatusCommitted(status, lsn)), ChangingProbe(change)) =>
      // FIXME: what do we do if correlationId or acknowledgementId are set?
      _correlationId = None
      _acknowledgementId = None
      children = change.children
      policy = change.policy
      behavior = change.behavior
      processor = behavior.makeProbeBehavior()
      processor.enter(this)
      unstashAll()
      goto(RunningProbe) using NoData

    case Event(Failure(failure: Throwable), _) =>
      throw failure

    case Event(other, _) =>
      // TODO: drop any messages of type Message?
      stash()
      stay()
  }

  onTransition {
    case _ -> ChangingProbe =>
      // ensure that timers are not running
      expiryTimer.stop()
      alertTimer.stop()
      // persist initializing state
      val timestamp = DateTime.now(DateTimeZone.UTC)
      _lifecycle = ProbeInitializing
      _health = ProbeUnknown
      _summary = None
      _lastChange = Some(timestamp)
      _lastUpdate = Some(timestamp)
      processor.exit(this)
      services.stateService.ask(ProbeState(getProbeStatus(timestamp), probeGeneration)).pipeTo(self)
  }


  /*
   * probe is transitioning from running to retired.  once the probe state is deleted
   * from the state service, the probe is stopped.
   */
  when(RetiringProbe) {
    case Event(Success(ProbeStatusCommitted(status, lsn)), _) =>
      context.stop(self)
      stay()

    case Event(Failure(failure: Throwable), _) =>
      throw failure

    case Event(other, _) =>
      stay()
  }

  onTransition {
    case _ -> RetiringProbe =>
      expiryTimer.stop()
      alertTimer.stop()
      processor.exit(this)
      services.stateService.ask(DeleteProbeState(probeRef, None, probeGeneration)).pipeTo(self)
  }

  /*
   * probe becomes Retired when it is determined to be stale; that is, the lsn from the
   * state service is newer than the probe generation.  when Retired, the probe ignores
   * all messages except for RetireProbe, which causes the Probe actor to stop.
   */
  when (StaleProbe) {

    case Event(RetireProbe(lsn), _) =>
      context.stop(self)
      stay()

    // ignore any other message
    case _ =>
      stay()
  }

  /**
   * ensure all timers are stopped, so we don't get spurious messages (and the corresponding
   * log messages in the debug log).
   */
  override def postStop(): Unit = {
    expiryTimer.stop()
    alertTimer.stop()
  }
}

object Probe {
  def props(probeRef: ProbeRef,
            parent: ActorRef,
            children: Set[ProbeRef],
            policy: ProbePolicy,
            behavior: ProbeBehavior,
            probeGeneration: Long,
            services: ServiceMap,
            metricsBus: MetricsBus) = {
    Props(classOf[Probe], probeRef, parent, children, policy, behavior, probeGeneration, services, metricsBus)
  }

  case class SendNotifications(notifications: Vector[Notification])
  case object ProbeAlertTimeout
  case object ProbeExpiryTimeout
}

sealed trait ProbeFSMState
case object InitializingProbe extends ProbeFSMState
case object ChangingProbe extends ProbeFSMState
case object RunningProbe extends ProbeFSMState
case object RetiringProbe extends ProbeFSMState
case object StaleProbe extends ProbeFSMState

sealed trait ProbeFSMData
case object NoData extends ProbeFSMData
case class ChangingProbe(command: ChangeProbe) extends ProbeFSMData
case class RunningProbe(inflight: Option[ProbeMutation], queued: Vector[Any]) extends ProbeFSMData
case class RetiringProbe(lsn: Long) extends ProbeFSMData

/* object lifecycle */
sealed trait ProbeLifecycle
case object ProbeInitializing extends ProbeLifecycle { override def toString = "initializing" }
case object ProbeJoining extends ProbeLifecycle { override def toString = "joining" }
case object ProbeKnown extends ProbeLifecycle   { override def toString = "known" }
case object ProbeSynthetic extends ProbeLifecycle { override def toString = "synthetic" }
case object ProbeRetired extends ProbeLifecycle { override def toString = "retired" }

/* object state */
sealed trait ProbeHealth
case object ProbeHealthy extends ProbeHealth  { override def toString = "healthy" }
case object ProbeDegraded extends ProbeHealth { override def toString = "degraded" }
case object ProbeFailed extends ProbeHealth   { override def toString = "failed" }
case object ProbeUnknown extends ProbeHealth  { override def toString = "unknown" }

/* */
case class ProbeStatus(probeRef: ProbeRef,
                       timestamp: DateTime,
                       lifecycle: ProbeLifecycle,
                       health: ProbeHealth,
                       summary: Option[String],
                       lastUpdate: Option[DateTime],
                       lastChange: Option[DateTime],
                       correlation: Option[UUID],
                       acknowledged: Option[UUID],
                       squelched: Boolean)

case class ProbeMetadata(probeRef: ProbeRef, metadata: Map[String,String])

case class ProbeMutation(status: ProbeStatus, notifications: Vector[Notification])

/* */
sealed trait ProbeOperation { val probeRef: ProbeRef }
sealed trait ProbeCommand extends ProbeOperation
sealed trait ProbeQuery extends ProbeOperation
case class ProbeOperationFailed(op: ProbeOperation, failure: Throwable)

case class GetProbeStatus(probeRef: ProbeRef) extends ProbeQuery
case class GetProbeStatusResult(op: GetProbeStatus, state: ProbeStatus)

case class SetProbeSquelch(probeRef: ProbeRef, squelch: Boolean) extends ProbeCommand
case class SetProbeSquelchResult(op: SetProbeSquelch, squelch: Boolean)

case class AcknowledgeProbe(probeRef: ProbeRef, correlationId: UUID) extends ProbeCommand
case class AcknowledgeProbeResult(op: AcknowledgeProbe, acknowledgementId: UUID)

case class AppendProbeWorknote(probeRef: ProbeRef, acknowledgementId: UUID, comment: String, internal: Option[Boolean]) extends ProbeCommand
case class AppendProbeWorknoteResult(op: AppendProbeWorknote, worknoteId: UUID)

case class UnacknowledgeProbe(probeRef: ProbeRef, acknowledgementId: UUID) extends ProbeCommand
case class UnacknowledgeProbeResult(op: UnacknowledgeProbe, acknowledgementId: UUID)
