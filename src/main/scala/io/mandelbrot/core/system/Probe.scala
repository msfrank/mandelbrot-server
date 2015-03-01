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
            val services: ActorRef,
            val metricsBus: MetricsBus) extends LoggingFSM[Probe.State,Probe.Data] with Stash with ProbeInterface {
  import Probe._

  // config
  val timeout = 5.seconds

  // state
  var seqNum: Long = 0
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
   * before starting the FSM, request the ProbeStatus from the state service.  the result
   * of this query determines which FSM state we transition to from Initializing.
   */
  override def preStart(): Unit = {
    // ask state service what our current status is
    val op = InitializeProbeStatus(probeRef, now)
    services ! op
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

    /* peek at internal state */
    case Event(query: GetProbeStatus, _) =>
      sender() ! GetProbeStatusResult(query, getProbeStatus)
      stay()

    /* peek at internal config */
    case Event(query: GetProbeConfig, _) =>
      sender() ! GetProbeConfigResult(query, children, policy, behavior)
      stay()

    /* ignore result if it doesn't match the in-flight request */
    case Event(result: InitializeProbeStatusResult, state: InitializingProbe) if result.op != state.command =>
      stay()

    case Event(result: InitializeProbeStatusResult, state: InitializingProbe) =>
      cancelCommitTimer()
      log.debug("gen {}: received initial status: {} (lsn {})", probeGeneration, result.status, result.seqNum)
      // initialize probe state
      setProbeStatus(result.status)
      seqNum = result.seqNum
      // so switch to retired behavior
      if (result.status.lifecycle == ProbeRetired) {
        log.debug("gen {}: probe becomes retired (lsn {})", probeGeneration, probeRef, seqNum)
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

    /* timed out waiting for initialization from state service */
    case Event(ProbeCommitTimeout, _) =>
      log.debug("gen {}: timeout while receiving initial state", probeGeneration)
      val op = InitializeProbeStatus(probeRef, now)
      services ! op
      setCommitTimer()
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

    /* peek at internal state */
    case Event(query: GetProbeStatus, _) =>
      sender() ! GetProbeStatusResult(query, getProbeStatus)
      stay()

    /* peek at internal config */
    case Event(query: GetProbeConfig, _) =>
      sender() ! GetProbeConfigResult(query, children, policy, behavior)
      stay()

    /* process a probe evaluation from the client */
    case Event(command: ProcessProbeEvaluation, state: RunningProbe) =>
      stay() using enqueue(state, QueuedCommand(command, sender()))

    /* if the probe behavior has changed, then transition to a new state */
    case Event(change: ChangeProbe, state: RunningProbe) =>
      stay() using enqueue(enqueue(state, QueuedEvent(ProbeExits, now)), QueuedChange(change, now))

    /* if the probe behavior has updated, then update our state */
    case Event(update: UpdateProbe, state: RunningProbe) =>
      stay() using enqueue(state, QueuedUpdate(update, now))

    /* if the probe behavior has retired, then update our state */
    case Event(retire: RetireProbe, state: RunningProbe) =>
      goto(RetiringProbe) using enqueue(state, QueuedRetire(retire, now))

    /* process child status and update state */
    case Event(event: ChildMutates, state: RunningProbe) =>
      if (children.contains(event.probeRef)) {
        stay() using enqueue(state, QueuedEvent(event, now))
      } else stay()

    /* process alert timeout and update state */
    case Event(ProbeAlertTimeout, state: RunningProbe) =>
      stay() using enqueue(state, QueuedEvent(ProbeAlertTimeout, now))

    /* process expiry timeout and update state */
    case Event(ProbeExpiryTimeout, state: RunningProbe) =>
      stay() using enqueue(state, QueuedEvent(ProbeExpiryTimeout, now))

    /* process probe commands */
    case Event(command: ProbeCommand, state: RunningProbe) =>
      stay() using enqueue(state, QueuedCommand(command, sender()))

    /* probe state has been committed, now we can apply the mutation */
    case Event(result: UpdateProbeStatusResult, state: RunningProbe) =>
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

    /* peek at internal state */
    case Event(query: GetProbeStatus, _) =>
      sender() ! GetProbeStatusResult(query, getProbeStatus)
      stay()

    /* peek at internal config */
    case Event(query: GetProbeConfig, _) =>
      sender() ! GetProbeConfigResult(query, children, policy, behavior)
      stay()

    /* probe state has been committed, now we can apply the mutation */
    case Event(result: UpdateProbeStatusResult, state: RunningProbe) =>
      state.inflight match {
        case None => stay()
        case Some(InflightMutation(op, _)) if result.op != op => stay()
        case _ => stay() using process(state)
      }

    /* probe delete has completed, stop the actor */
    case Event(result: DeleteProbeStatusResult, state: RunningProbe) =>
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

  /* shortcut to get the current time */
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

    // consume queued messages until we find one to process
    while (queued.nonEmpty) {
      // mutation will contain Some(result) from message processing, or None
      val maybeMutation: Option[Mutation] = queued.head match {

        // process the ProbeCommand
        case QueuedCommand(command, caller) =>
          processor.processCommand(this, command) match {
            case Success(effect) =>
              Some(CommandMutation(caller, effect.result, effect.status, effect.notifications))
            case Failure(ex) =>
              caller ! ProbeOperationFailed(command, ex)
              None
          }

        // process the ProbeEvent
        case QueuedEvent(event, timestamp) =>
          processor.processEvent(this, event).map { effect =>
            EventMutation(effect.status, effect.notifications)
          }

        case QueuedUpdate(update, timestamp) =>
          processor.update(this, update.behavior) match {
            case Some(EventEffect(status, notifications)) =>
              Some(ConfigMutation(update.children, update.policy, update.behavior, status, notifications))
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
          processor = behavior.makeProbeBehavior()
          processor.enter(this).map { effect =>
            EventMutation(effect.status, effect.notifications)
          }

        case QueuedRetire(retire, timestamp) =>
          processor.exit(this) match {
            case Some(EventEffect(status, notifications)) =>
              Some(Deletion(Some(status), notifications, retire.lsn))
            case None =>
              Some(Deletion(None, Vector.empty, retire.lsn))
          }
      }

      maybeMutation match {
        case None =>
          log.debug("skipping {}", queued.head)
          queued = queued.tail
        case Some(mutation: StatusMutation) =>
          val op = UpdateProbeStatus(probeRef, mutation.status, filterNotifications(mutation.notifications), seqNum)
          services ! op
          setCommitTimer()
          log.debug("op {} is in flight with mutation {}", queued.head, mutation)
          return RunningProbe(Some(InflightMutation(op, mutation)), queued)
        case Some(deletion: Deletion) =>
          val op = DeleteProbeStatus(probeRef, deletion.lastStatus, seqNum)
          services ! op
          setCommitTimer()
          log.debug("op {} is in flight with mutation {}", queued.head, deletion)
          return RunningProbe(Some(InflightMutation(op, deletion)), queued)
      }
    }
    RunningProbe(None, queued)
  }

  /**
   * The in-flight message has been persisted, so allow the probe to process it.
   */
  private def process(state: RunningProbe): RunningProbe = {
    cancelCommitTimer()
    log.debug("processing in flight mutation {}", state.inflight)
    // apply the mutation to the probe
    val notifications: Vector[ProbeNotification] = state.inflight match {
      case None => Vector.empty
      case Some(InflightMutation(op, event: EventMutation)) =>
        setProbeStatus(event.status)
        parent ! ChildMutates(probeRef, event.status)
        event.notifications
      case Some(InflightMutation(op, command: CommandMutation)) =>
        setProbeStatus(command.status)
        parent ! ChildMutates(probeRef, command.status)
        command.caller ! command.result
        command.notifications
      case Some(InflightMutation(op, config: ConfigMutation)) =>
        children = config.children
        policy = config.policy
        behavior = config.behavior
        setProbeStatus(config.status)
        processor = behavior.makeProbeBehavior()
        parent ! ChildMutates(probeRef, config.status)
        config.notifications
      case Some(InflightMutation(op, deletion: Deletion)) =>
        deletion.lastStatus.foreach { status =>
          setProbeStatus(status)
          parent ! ChildMutates(probeRef, status)
        }
        deletion.notifications
    }
    // send notifications
    notifications.foreach { notification => services ! notification }
    // process the next queued message
    persist(RunningProbe(None, state.queued.tail))
  }

  /**
   * The in-flight message has failed to persist, so perform recovery.  Right now, we
   * don't actually retry, we just drop the in-flight message and start processing the
   * next one.
   */
  private def recover(state: RunningProbe): RunningProbe = {
    cancelCommitTimer()
    // FIXME: implement some retry policy here
    persist(RunningProbe(None, state.queued.tail))
  }

  /**
   * if the probe message queue is empty then immediately start processing the message,
   * otherwise append the message to the queue.
   */
  private def enqueue(state: RunningProbe, message: QueuedMessage): RunningProbe = {
    val updated = RunningProbe(state.inflight, state.queued :+ message)
    if (state.inflight.isDefined) updated else persist(updated)
  }

  /**
   * filter out notifications if they don't match the current policy
   */
  def filterNotifications(notifications: Vector[ProbeNotification]): Vector[ProbeNotification] = notifications.filter {
    // always allow alerts
    case alert: Alert => true
    // if there is no explicit policy, or the kind matches the current policy, then allow
    case notification: ProbeNotification =>
      policy.notifications match {
        case None => true
        case Some(kind) if kind.contains(notification.kind) => true
        case _ => false
      }
    // drop anything else
    case _ => false
  }

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
            services: ActorRef,
            metricsBus: MetricsBus) = {
    Props(classOf[Probe], probeRef, parent, children, policy, behavior, probeGeneration, services, metricsBus)
  }

  case class InflightMutation(op: StateServiceCommand, mutation: Mutation)

  sealed trait State
  case object InitializingProbe extends State
  case object ChangingProbe extends State
  case object RunningProbe extends State
  case object RetiringProbe extends State
  case object StaleProbe extends State

  sealed trait Data
  case class InitializingProbe(command: InitializeProbeStatus) extends Data
  case class ChangingProbe(command: ChangeProbe, inflight: InflightMutation, queued: Vector[QueuedMessage]) extends Data
  case class RunningProbe(inflight: Option[InflightMutation], queued: Vector[QueuedMessage]) extends Data
  case class RetiringProbe(inflight: InflightMutation, lsn: Long) extends Data
  case object NoData extends Data

  sealed trait QueuedMessage
  case class QueuedEvent(event: ProbeEvent, timestamp: DateTime) extends QueuedMessage
  case class QueuedCommand(command: ProbeCommand, caller: ActorRef) extends QueuedMessage
  case class QueuedUpdate(update: UpdateProbe, timestamp: DateTime) extends QueuedMessage
  case class QueuedChange(change: ChangeProbe, timestamp: DateTime) extends QueuedMessage
  case class QueuedRetire(retire: RetireProbe, timestamp: DateTime) extends QueuedMessage

  sealed trait Mutation { val notifications: Vector[ProbeNotification] }
  sealed trait StatusMutation extends Mutation { val status: ProbeStatus }
  case class CommandMutation(caller: ActorRef, result: Any, status: ProbeStatus, notifications: Vector[ProbeNotification]) extends StatusMutation
  case class EventMutation(status: ProbeStatus, notifications: Vector[ProbeNotification]) extends StatusMutation
  case class ConfigMutation(children: Set[ProbeRef], policy: ProbePolicy, behavior: ProbeBehavior, status: ProbeStatus, notifications: Vector[ProbeNotification]) extends StatusMutation
  case class Deletion(lastStatus: Option[ProbeStatus], notifications: Vector[ProbeNotification], lsn: Long) extends Mutation
}

sealed trait ProbeEvent
case class ChildMutates(probeRef: ProbeRef, status: ProbeStatus) extends ProbeEvent
case object ProbeEnters extends ProbeEvent
case object ProbeExits extends ProbeEvent
case object ProbeCommitTimeout extends ProbeEvent
case object ProbeAlertTimeout extends ProbeEvent
case object ProbeExpiryTimeout extends ProbeEvent

/* */
trait ProbeOperation extends ServiceOperation { val probeRef: ProbeRef }
sealed trait ProbeCommand extends ServiceCommand with ProbeOperation
sealed trait ProbeQuery extends ServiceQuery with ProbeOperation
case class ProbeOperationFailed(op: ProbeOperation, failure: Throwable) extends ServiceOperationFailed

case class GetProbeStatus(probeRef: ProbeRef) extends ProbeQuery
case class GetProbeStatusResult(op: GetProbeStatus, status: ProbeStatus)

case class GetProbeConfig(probeRef: ProbeRef) extends ProbeQuery
case class GetProbeConfigResult(op: GetProbeConfig, children: Set[ProbeRef], policy: ProbePolicy, behavior: ProbeBehavior)

case class ProcessProbeEvaluation(probeRef: ProbeRef, evaluation: ProbeEvaluation) extends ProbeCommand
case class ProcessProbeEvaluationResult(op: ProcessProbeEvaluation)

case class SetProbeSquelch(probeRef: ProbeRef, squelch: Boolean) extends ProbeCommand
case class SetProbeSquelchResult(op: SetProbeSquelch, squelch: Boolean)

case class AcknowledgeProbe(probeRef: ProbeRef, correlationId: UUID) extends ProbeCommand
case class AcknowledgeProbeResult(op: AcknowledgeProbe, acknowledgementId: UUID)

case class UnacknowledgeProbe(probeRef: ProbeRef, acknowledgementId: UUID) extends ProbeCommand
case class UnacknowledgeProbeResult(op: UnacknowledgeProbe, acknowledgementId: UUID)
