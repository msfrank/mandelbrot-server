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
import io.mandelbrot.core.registry._
import io.mandelbrot.core.notification._
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
            var children: Set[ProbeRef],
            var policy: ProbePolicy,
            var behavior: ProbeBehavior,
            val probeGeneration: Long,
            val services: ServiceMap,
            val metricsBus: MetricsBus) extends LoggingFSM[ProbeFSMState,ProbeFSMData] with Stash with ProbeInterface {
  import Probe._

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

  /**
   * before starting the FSM, request the ProbeState from the state service.  the result
   * of this query determines which FSM state we transition to from Initializing.
   */
  override def preStart(): Unit = {
    // ask state service what our current status is
    val op = InitializeProbeState(probeRef, now, probeGeneration)
    services.stateService ! op
    setCommitTimer()
    startWith(InitializingProbe, InitializingProbe(op))
    /* FSM initialization is the last step in the constructor */
    initialize()
  }

  /*
   * wait for ProbeState from state service.  if the lsn returned equals the probe
   * generation, then transition to the appropriate behavior, otherwise transition
   * directly to Retired.
   */
  when(InitializingProbe) {

    case Event(result: InitializeProbeStateResult, state: InitializingProbe) if result.op != state.command =>
      stay()

    case Event(InitializeProbeStateResult(op, status, lsn), state: InitializingProbe) =>
      log.debug("gen {}: received initial state: {} (lsn {})", probeGeneration, status, lsn)
      // initialize probe state
      setProbeStatus(status)
      // this generation is not current, so switch to retired behavior
      if (lsn > probeGeneration) {
        log.debug("gen {}: probe becomes retired (lsn {})", probeGeneration, probeRef, lsn)
        goto(StaleProbe) using NoData
      }
      // otherwise replay any stashed messages and transition to initialized
      else {
        unstashAll()
        goto(RunningProbe) using enqueue(RunningProbe(None, Vector.empty), QueuedEvent(ProbeEnters, now))
      }

    case Event(result: StateServiceOperationFailed, state: InitializingProbe) if result.op != state.command =>
      stay()

    case Event(StateServiceOperationFailed(_, failure: ApiException), _) if failure.failure == ResourceNotFound =>
      log.debug("probe {} becomes retired", probeRef)
      cancelCommitTimer()
      goto(StaleProbe) using NoData

    case Event(StateServiceOperationFailed(op, failure), _) =>
      log.debug("gen {}: failure receiving initial state: {}", probeGeneration, failure)
      cancelCommitTimer()
      throw failure

    case Event(ProbeCommitTimeout, _) =>
      log.debug("gen {}: timeout while receiving initial state", probeGeneration)
      val op = InitializeProbeState(probeRef, now, probeGeneration)
      services.stateService ! op
      setCommitTimer()
      stay() using InitializingProbe(op)

    case Event(other, _) =>
      // TODO: drop any messages of type Message?
      stash()
      stay()
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
    case Event(change: ChangeProbe, state: RunningProbe) =>
      stay() using enqueue(enqueue(state, QueuedEvent(ProbeExits, now)), QueuedChange(change, now))

    /* if the probe behavior has updated, then update our state */
    case Event(update: UpdateProbe, state: RunningProbe) =>
      stay() using enqueue(state, QueuedUpdate(update, now))

    /* if the probe behavior has retired, then update our state */
    case Event(retire: RetireProbe, state: RunningProbe) =>
      goto(RetiringProbe) using enqueue(state, QueuedRetire(retire, now))

    /* process status message and update state */
    case Event(message: StatusMessage, state: RunningProbe) =>
      stay() using enqueue(state, QueuedEvent(message, now))

    /* process metrics message and update state */
    case Event(message: MetricsMessage, state: RunningProbe) =>
      stay() using enqueue(state, QueuedEvent(message, now))

    /* process child status and update state */
    case Event(message: ProbeStatus, state: RunningProbe) =>
      if (!children.contains(message.probeRef)) stay() else {
        stay() using enqueue(state, QueuedEvent(message, now))
      }

    /* process alert timeout and update state */
    case Event(message @ ProbeAlertTimeout, state: RunningProbe) =>
      stay() using enqueue(state, QueuedEvent(message, now))

    /* process expiry timeout and update state */
    case Event(message @ ProbeExpiryTimeout, state: RunningProbe) =>
      stay() using enqueue(state, QueuedEvent(message, now))

    /* process probe commands */
    case Event(command: ProbeCommand, state: RunningProbe) =>
      stay() using enqueue(state, QueuedCommand(command, sender()))

    /* probe state has been committed, now we can apply the mutation */
    case Event(result: UpdateProbeStateResult, state: RunningProbe) =>
      state.inflight match {
        case None => stay()
        case Some(InflightMutation(op, _)) if result.op != op => stay()
        case _ => stay() using process(state)
      }

    /* state failed to commit */
    case Event(failure: StateServiceOperationFailed, state: RunningProbe) =>
      state.inflight match {
        case None => stay()
        case Some(InflightMutation(op, _)) if failure.op != op => stay()
        case _ =>
          log.warning("failed to commit status for {}: {}", probeRef, failure.failure)
          stay() using recover(state)
      }

    /* timeout waiting to commit */
    case Event(ProbeCommitTimeout, state: RunningProbe) =>
      log.debug("timeout committing status for {}", probeRef)
      stay() using recover(state)
  }

  /*
   * probe is transitioning from running to retired.  once the probe state is deleted
   * from the state service, the probe is stopped.
   */
  when(RetiringProbe) {

    /* probe state has been committed, now we can apply the mutation */
    case Event(result: UpdateProbeStateResult, state: RunningProbe) =>
      state.inflight match {
        case None => stay()
        case Some(InflightMutation(op, _)) if result.op != op => stay()
        case _ => stay() using process(state)
      }

    /* probe delete has completed, stop the actor */
    case Event(result: DeleteProbeStateResult, state: RunningProbe) =>
      state.inflight match {
        case None => stay()
        case Some(InflightMutation(op, _)) if result.op != op => stay()
        case _ => stop() using process(state)
      }

    /* state failed to commit */
    case Event(failure: StateServiceOperationFailed, state: RunningProbe) =>
      state.inflight match {
        case None => stay()
        case Some(InflightMutation(op, _)) if failure.op != op => stay()
        case _ =>
          log.warning("failed to commit status for {}: {}", probeRef, failure.failure)
          stay() using recover(state)
      }

    /* timeout waiting to commit */
    case Event(ProbeCommitTimeout, state: RunningProbe) =>
      log.debug("timeout committing status for {}", probeRef)
      stay() using recover(state)

    /* drop any other messages */
    case Event(other, _) =>
      stay()
  }

  /* implement accessors for ProbeInterface */
  def lifecycle: ProbeLifecycle = _lifecycle
  def health: ProbeHealth = _health
  def summary: Option[String] = _summary
  def lastChange: Option[DateTime] = _lastChange
  def lastUpdate: Option[DateTime] = _lastUpdate
  def correlationId: Option[UUID] = _correlationId
  def acknowledgementId: Option[UUID] = _acknowledgementId
  def squelch: Boolean = _squelch

  /**
   * set internal state based on the specified ProbeStatus.
   */
  def setProbeStatus(status: ProbeStatus): Unit = {
      _lifecycle = status.lifecycle
      _health = status.health
      _summary = status.summary
      _lastChange = status.lastChange
      _lastUpdate = status.lastUpdate
      _correlationId = status.correlation
      _acknowledgementId = status.acknowledged
      _squelch = status.squelched
  }

  def now = DateTime.now(DateTimeZone.UTC)
  
  /* set/reset the commit timer */
  private def setCommitTimer() = setTimer("commit", ProbeCommitTimeout, timeout)

  /* cancel the commit timer */
  private def cancelCommitTimer() = cancelTimer("commit")

  /**
   *
   */
  private def persist(state: RunningProbe): RunningProbe = {
    var queued = state.queued
    while (queued.nonEmpty) {
      val mutation: Option[Mutation] = queued.head match {
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
        case QueuedUpdate(update, timestamp) =>
          processor.update(this, update.behavior) match {
            case Some(EventMutation(status, notifications)) => processor.update(this, update.behavior)
              Some(ConfigMutation(update.children, update.policy, update.behavior, status))
            case None =>
              children = update.children
              policy = update.policy
              behavior = update.behavior
              None
          }
        case QueuedChange(change, timestamp) =>
          children = change.children
          policy = change.policy
          behavior = change.behavior
          processor.enter(this)
        case QueuedRetire(retire, timestamp) =>
          processor.exit(this) match {
            case Some(EventMutation(status, notifications)) =>
              Some(Deletion(Some(status), notifications, retire.lsn))
            case None =>
              Some(Deletion(None, Vector.empty, retire.lsn))
          }
      }
      mutation match {
        case None =>
          queued = queued.tail
        case Some(mutation: UpdateMutation) =>
          val op = UpdateProbeState(probeRef, mutation.status, probeGeneration)
          services.stateService ! op
          setCommitTimer()
          return RunningProbe(Some(InflightMutation(op, mutation)), queued)
        case Some(deletion: Deletion) =>
          val op = DeleteProbeState(probeRef, deletion.lastStatus, deletion.lsn)
          services.stateService ! op
          setCommitTimer()
          return RunningProbe(Some(InflightMutation(op, deletion)), queued)
      }
    }
    RunningProbe(None, queued)
  }

  private def process(state: RunningProbe): RunningProbe = {
    cancelCommitTimer()
    state.inflight match {
      case None =>  // do nothing
        log.debug("process None: {}", state.inflight)
      case Some(InflightMutation(op, EventMutation(status, notifications))) =>
        log.debug("process event: {}", state.inflight)
        setProbeStatus(status)
        parent ! status
        sendNotifications(notifications)
      case Some(InflightMutation(op, CommandMutation(result, status, notifications))) =>
        log.debug("process command: {}", state.inflight)
        setProbeStatus(status)
        parent ! status
        // FIXME: blegh this is ugly
        state.queued.head.asInstanceOf[QueuedCommand].caller ! result
        sendNotifications(notifications)
      case Some(InflightMutation(op, config: ConfigMutation)) =>
        log.debug("process config: {}", state.inflight)
        children = config.children
        policy = config.policy
        behavior = config.behavior
        setProbeStatus(config.status)
        processor = behavior.makeProbeBehavior()
        parent ! config.status
      case Some(InflightMutation(op, deletion: Deletion)) =>
        log.debug("process delete: {}", state.inflight)
        deletion.lastStatus.foreach { status =>
          setProbeStatus(status)
          parent ! status
        }
        sendNotifications(deletion.notifications)
    }
    persist(RunningProbe(None, state.queued.tail))
  }

  private def recover(state: RunningProbe): RunningProbe = {
    cancelCommitTimer()
    // FIXME: implement some retry policy here
    persist(RunningProbe(None, state.queued.tail))
  }

  private def enqueue(state: RunningProbe, message: QueuedMessage): RunningProbe = {
    val updated = RunningProbe(state.inflight, state.queued :+ message)
    if (state.inflight.isDefined) updated else persist(updated)
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
   * subscribe probe to all metrics at the specified probe path.
   */
  def subscribeToMetrics(probePath: Vector[String]): Unit = metricsBus.subscribe(self, probePath)

  /**
   * unsubscribe probe from all metrics at the specified probe path.
   */
  def unsubscribeFromMetrics(probePath: Vector[String]): Unit = metricsBus.unsubscribe(self, probePath)

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

  case object ProbeEnters
  case object ProbeExits
  case object ProbeCommitTimeout
  case object ProbeAlertTimeout
  case object ProbeExpiryTimeout
}

/* */
sealed trait Mutation
sealed trait UpdateMutation extends Mutation { val status: ProbeStatus }
case class CommandMutation(result: Any, status: ProbeStatus, notifications: Vector[Notification]) extends UpdateMutation
case class EventMutation(status: ProbeStatus, notifications: Vector[Notification]) extends UpdateMutation
case class ConfigMutation(children: Set[ProbeRef], policy: ProbePolicy, behavior: ProbeBehavior, status: ProbeStatus) extends UpdateMutation
case class Deletion(lastStatus: Option[ProbeStatus], notifications: Vector[Notification], lsn: Long) extends Mutation

case class InflightMutation(op: StateServiceCommand, mutation: Mutation)

sealed trait QueuedMessage
case class QueuedCommand(command: ProbeCommand, caller: ActorRef) extends QueuedMessage
case class QueuedEvent(event: Any, timestamp: DateTime) extends QueuedMessage
case class QueuedUpdate(update: UpdateProbe, timestamp: DateTime) extends QueuedMessage
case class QueuedChange(change: ChangeProbe, timestamp: DateTime) extends QueuedMessage
case class QueuedRetire(retire: RetireProbe, timestamp: DateTime) extends QueuedMessage

sealed trait ProbeFSMState
case object InitializingProbe extends ProbeFSMState
case object ChangingProbe extends ProbeFSMState
case object RunningProbe extends ProbeFSMState
case object RetiringProbe extends ProbeFSMState
case object StaleProbe extends ProbeFSMState

sealed trait ProbeFSMData
case class InitializingProbe(command: InitializeProbeState) extends ProbeFSMData
case class ChangingProbe(command: ChangeProbe, inflight: InflightMutation, queued: Vector[QueuedMessage]) extends ProbeFSMData
case class RunningProbe(inflight: Option[InflightMutation], queued: Vector[QueuedMessage]) extends ProbeFSMData
case class RetiringProbe(inflight: InflightMutation, lsn: Long) extends ProbeFSMData
case object NoData extends ProbeFSMData

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
