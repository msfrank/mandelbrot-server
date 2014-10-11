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
import io.mandelbrot.core._
import io.mandelbrot.core.metrics.MetricsBus
import io.mandelbrot.core.util.Timer

import scala.util.{Try, Failure, Success}

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
  val timeout = 5.seconds

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

  private def setCommitTimer() = setTimer("commit", ProbeCommitTimeout, timeout)
  private def cancelCommitTimer() = cancelTimer("commit")

  /**
   * before starting the FSM, request the ProbeState from the state service.  the result
   * of this query determines which FSM state we transition to from Initializing.
   */
  override def preStart(): Unit = {
    // ask state service what our current status is
    services.stateService ! InitializeProbeState(probeRef, DateTime.now(DateTimeZone.UTC), probeGeneration)
    setCommitTimer()
  }

  /*
   * wait for ProbeState from state service.  if the lsn returned equals the probe
   * generation, then transition to the appropriate behavior, otherwise transition
   * directly to Retired.
   */
  when(InitializingProbe) {

    case Event(ProbeState(status, lsn), _) =>
      log.debug("gen {}: received initial state: {} (lsn {})", probeGeneration, status, lsn)
      // initialize probe state
      setProbeStatus(status)
      // this generation is not current, so switch to retired behavior
      if (lsn > probeGeneration) {
        log.debug("gen {}: probe becomes retired (lsn {})", probeGeneration, probeRef, lsn)
        goto(StaleProbe) using NoData
      }
      // otherwise replay any stashed messages and transition to initialized
      else
        goto(RunningProbe) using enqueue(RunningProbe(None, Vector.empty), QueuedEvent(ProbeEnters, DateTime.now(DateTimeZone.UTC)))

    case Event(failure: ApiException, _) if failure.failure == ResourceNotFound =>
      log.debug("probe {} becomes retired", probeRef)
      cancelCommitTimer()
      goto(StaleProbe) using NoData

    case Event(ProbeCommitTimeout, _) =>
      log.debug("gen {}: timeout while receiving initial state", probeGeneration)
      services.stateService ! InitializeProbeState(probeRef, DateTime.now(DateTimeZone.UTC), probeGeneration)
      setCommitTimer()
      stay()

    case Event(failure: Throwable, _) =>
      log.debug("gen {}: failure receiving initial state: {}", probeGeneration, failure)
      cancelCommitTimer()
      throw failure

    case Event(other, _) =>
      // TODO: drop any messages of type Message?
      stash()
      stay()
  }

  private def persist(state: RunningProbe): RunningProbe = {
    var queued = state.queued
    while (queued.nonEmpty) {
      val mutation: Option[InflightMutation] = queued.head match {
        case QueuedCommand(command, caller) =>
          processor.processCommand(this, command) match {
            case Success(result) =>
              Some(result)
            case Failure(ex) =>
              caller ! ProbeOperationFailed(command, ex)
              None
          }
        case QueuedEvent(event, timestamp) =>
          processor.processEvent(this, event)
      }
      mutation match {
        case None =>
          queued = queued.tail
        case mutation @ Some(CommandMutation( _, status, _)) =>
          services.stateService ! ProbeState(status, probeGeneration)
          setCommitTimer()
          return RunningProbe(mutation, queued)
        case mutation @ Some(EventMutation(status, _)) =>
          services.stateService ! ProbeState(status, probeGeneration)
          setCommitTimer()
          return RunningProbe(mutation, queued)
      }
    }
    RunningProbe(None, queued)
  }

  private def enqueue(state: RunningProbe, message: QueuedMessage): RunningProbe = {
    val updated = RunningProbe(state.inflight, state.queued :+ message)
    if (state.inflight.isDefined) updated else persist(updated)
  }

  private def process(state: RunningProbe): RunningProbe = {
    cancelCommitTimer()
    state.inflight match {
      case Some(EventMutation(status, notifications)) =>
        setProbeStatus(status)
        parent ! status
        sendNotifications(notifications)
      case Some(CommandMutation(result, status, notifications)) =>
        setProbeStatus(status)
        // blegh this is ugly
        state.queued.head.asInstanceOf[QueuedCommand].caller ! result
        sendNotifications(notifications)
    }
    persist(RunningProbe(None, state.queued.tail))
  }

  private def recover(state: RunningProbe): RunningProbe = {
    cancelCommitTimer()
    // FIXME: implement some retry policy here
    persist(RunningProbe(None, state.queued.tail))
  }

  /*
   * probe is ready to process messages.
   */
  when(RunningProbe) {

    /* peek at internals */
    case Event(query: GetProbeStatus, _) =>
      sender() ! GetProbeStatusResult(query, getProbeStatus)
      stay()

    /* if the probe behavior has changed, then transition to a new state */
    case Event(change: ChangeProbe, RunningProbe(inflight, queued)) =>
      goto(ChangingProbe) using ChangingProbe(change, inflight, queued)

    /* if the probe behavior has updated, then update our state */
    case Event(update: UpdateProbe, _) =>
      children = update.children
      policy = update.policy
      behavior = update.behavior
      processor.update(this, behavior)
      stay()

    /* process status message and update state */
    case Event(message: StatusMessage, state: RunningProbe) =>
      stay() using enqueue(state, QueuedEvent(message, DateTime.now(DateTimeZone.UTC)))

    /* process metrics message and update state */
    case Event(message: MetricsMessage, state: RunningProbe) =>
      stay() using enqueue(state, QueuedEvent(message, DateTime.now(DateTimeZone.UTC)))

    /* process child status and update state */
    case Event(message: ProbeStatus, state: RunningProbe) =>
      if (!children.contains(message.probeRef)) stay() else {
        stay() using enqueue(state, QueuedEvent(message, DateTime.now(DateTimeZone.UTC)))
      }

    /* process alert timeout and update state */
    case Event(message @ ProbeAlertTimeout, state: RunningProbe) =>
      stay() using enqueue(state, QueuedEvent(message, DateTime.now(DateTimeZone.UTC)))

    /* process expiry timeout and update state */
    case Event(message @ ProbeExpiryTimeout, state: RunningProbe) =>
      stay() using enqueue(state, QueuedEvent(message, DateTime.now(DateTimeZone.UTC)))

    /* process probe commands */
    case Event(command: ProbeCommand, state: RunningProbe) =>
      stay() using enqueue(state, QueuedCommand(command, sender()))

    /* probe state has been committed, now we can apply the mutation */
    case Event(committed: ProbeStatusCommitted, state: RunningProbe) =>
      stay() using process(state)

    /* timeout waiting to commit */
    case Event(ProbeCommitTimeout, state: RunningProbe) =>
      log.warning("timeout committing status for {}", probeRef)
      stay() using recover(state)

    /* state failed to commit */
    case Event(ex: Throwable, state: RunningProbe) =>
      log.warning("failed to commit status for {}: {}", probeRef, ex)
      stay() using recover(state)

    /* move to retired state */
    case Event(RetireProbe(lsn), _) =>
      goto(RetiringProbe) using RetiringProbe(lsn)
  }

  onTransition {
    case _ -> RunningProbe => unstashAll()
  }

  /*
   * probe is transitioning between behaviors.  we change probe state to Initializing and
   * commit it to the state service.  once the change has been committed, we transition to
   * the new behavior.
   */
  when(ChangingProbe) {

    case Event(ProbeStatusCommitted(status, lsn), ChangingProbe(change, _, queued)) =>
      // FIXME: what do we do if correlationId or acknowledgementId are set?
      _correlationId = None
      _acknowledgementId = None
      children = change.children
      policy = change.policy
      behavior = change.behavior
      processor = behavior.makeProbeBehavior()
      processor.enter(this)
      unstashAll()
      goto(RunningProbe) using RunningProbe(None, queued)

    case Event(ProbeCommitTimeout, _) =>
      log.debug("gen {}: timeout while receiving initial state", probeGeneration)
      services.stateService ! ProbeState(getProbeStatus(lastChange.get), probeGeneration)
      setCommitTimer()
      stay()

    case Event(failure: Throwable, _) =>
      log.debug("gen {}: failure changing state: {}", probeGeneration, failure)
      cancelCommitTimer()
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
      cancelCommitTimer()
      // persist initializing state
      val timestamp = DateTime.now(DateTimeZone.UTC)
      _lifecycle = ProbeInitializing
      _health = ProbeUnknown
      _summary = None
      _lastChange = Some(timestamp)
      _lastUpdate = Some(timestamp)
      processor.exit(this)
      services.stateService ! ProbeState(getProbeStatus(timestamp), probeGeneration)
      setCommitTimer()
  }


  /*
   * probe is transitioning from running to retired.  once the probe state is deleted
   * from the state service, the probe is stopped.
   */
  when(RetiringProbe) {
    case Event(ProbeStatusCommitted(status, lsn), _) =>
      context.stop(self)
      stay()

    case Event(ProbeCommitTimeout, _) =>
      log.debug("gen {}: timeout while deleting state", probeGeneration)
      services.stateService ! DeleteProbeState(probeRef, None, probeGeneration)
      setCommitTimer()
      stay()

    case Event(Failure(failure: Throwable), _) =>
      log.debug("gen {}: failure deleting state: {}", probeGeneration, failure)
      cancelCommitTimer()
      throw failure

    case Event(other, _) =>
      stay()
  }

  onTransition {
    case _ -> RetiringProbe =>
      expiryTimer.stop()
      alertTimer.stop()
      processor.exit(this)
      services.stateService ! DeleteProbeState(probeRef, None, probeGeneration)
      setCommitTimer()
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

  onTransition {
    case _ -> StaleProbe => unstashAll()
  }

  /**
   * send the notification if the notification set policy is not specified (meaning
   * send all notifications) or if the policy is specified and this specific notification
   * type is in the notification set.
   */
  def sendNotification(notification: Notification): Unit = notification match {
    case alert: Alert =>
      log.debug("sending alert {}", notification)
      services.notificationService ! alert
    case _ if policy.notifications.isEmpty || policy.notifications.get.contains(notification.kind) =>
      log.debug("sending notification {}", notification)
      services.notificationService ! notification
    case _ => // drop notification
  }

  def sendNotifications(notifications: Iterable[Notification]): Unit = notifications.foreach(sendNotification)

  /**
   * ensure all timers are stopped, so we don't get spurious messages (and the corresponding
   * log messages in the debug log).
   */
  override def postStop(): Unit = {
    expiryTimer.stop()
    alertTimer.stop()
  }

  /* FSM initialization is the last step in the constructor */
  initialize()
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

  case object ProbeEnters
  case object ProbeExits
  case object ProbeCommitTimeout
  case object ProbeAlertTimeout
  case object ProbeExpiryTimeout
}

/* */
sealed trait InflightMutation
case class CommandMutation(result: Any, status: ProbeStatus, notifications: Vector[Notification]) extends InflightMutation
case class EventMutation(status: ProbeStatus, notifications: Vector[Notification]) extends InflightMutation

sealed trait QueuedMessage
case class QueuedCommand(command: ProbeCommand, caller: ActorRef) extends QueuedMessage
case class QueuedEvent(event: Any, timestamp: DateTime) extends QueuedMessage

sealed trait ProbeFSMState
case object InitializingProbe extends ProbeFSMState
case object ChangingProbe extends ProbeFSMState
case object RunningProbe extends ProbeFSMState
case object RetiringProbe extends ProbeFSMState
case object StaleProbe extends ProbeFSMState

sealed trait ProbeFSMData
case object NoData extends ProbeFSMData
case class ChangingProbe(command: ChangeProbe, inflight: Option[InflightMutation], queued: Vector[QueuedMessage]) extends ProbeFSMData
case class RunningProbe(inflight: Option[InflightMutation], queued: Vector[QueuedMessage]) extends ProbeFSMData
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
