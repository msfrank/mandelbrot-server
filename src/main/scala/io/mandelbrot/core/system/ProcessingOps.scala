package io.mandelbrot.core.system

import akka.actor.{Actor, ActorRef}
import akka.event.LoggingAdapter
import io.mandelbrot.core.state.UpdateProbeStatus
import org.joda.time.DateTime
import scala.util.{Failure, Success}

import io.mandelbrot.core.model._

/**
 *
 */
trait ProcessingOps extends Actor with MutationOps {

  implicit def log: LoggingAdapter

  val services: ActorRef

  var probeType: String
  var policy: ProbePolicy
  var processor: BehaviorProcessor
  var children: Set[ProbeRef]
  var lastCommitted: Option[DateTime]

  private var inflight: Option[(Mutation,UpdateProbeStatus)] = None
  private var queued: Vector[QueuedMessage] = Vector.empty

  /**
   * if the message queue is empty then immediately start processing the message,
   * otherwise append the message to the queue.
   */
  def enqueue(message: QueuedMessage): Option[UpdateProbeStatus] = {
    queued = queued :+ message
    log.debug("enqueued:\n\n    {}\n", message)
    if (inflight.isEmpty) process() else None
  }

  /**
   * process the current message from the queue.  if processing results in a mutation,
   * then return the mutation, otherwise loop until a mutation is generated or the queue
   * is exhausted.
   */
  def process(): Option[UpdateProbeStatus] = {
    // consume queued messages until we find one to process
    while (queued.nonEmpty) {
      // mutation will contain Some(result) from message processing, or None
      val maybeMutation: Option[Mutation] = queued.head match {

        // process the ProbeCommand
        case QueuedCommand(command, caller) =>
          processor.processCommand(this, command) match {
            case Success(effect) =>
              val notifications = filterNotifications(effect.notifications)
              Some(CommandMutation(caller, effect.result, effect.status, notifications))
            case Failure(ex) =>
              caller ! ProbeOperationFailed(command, ex)
              None
          }

        // process the ProbeEvent
        case QueuedEvent(event, timestamp) =>
          processor.processEvent(this, event).map { effect =>
            val notifications = filterNotifications(effect.notifications)
            EventMutation(effect.status, notifications)
          }

        // the processor has changed
        case QueuedChange(change, timestamp) =>
          val _processor = change.factory.implement()
          var initial: ProbeStatus = getProbeStatus
          if (!change.probeType.equals(probeType)) {
            initial = initial.copy(lifecycle = ProbeInitializing, health = ProbeUnknown)
          }
          val effect = _processor.configure(initial, change.children)
          val notifications = filterNotifications(effect.notifications)
          Some(ConfigMutation(change.policy, effect.children, effect.metrics, effect.status, notifications))

        // the probe is retiring
        case QueuedRetire(retire, timestamp) =>
          processor.retire(this, retire.lsn).map { effect =>
            val notifications = filterNotifications(effect.notifications)
            Deletion(effect.status, notifications, retire.lsn)
          }
      }

      maybeMutation match {
        case None =>
          queued = queued.tail
        case Some(mutation: Mutation) =>
          val op = UpdateProbeStatus(probeRef, mutation.status, mutation.notifications, lastCommitted)
          services ! op
          inflight = Some(mutation -> op)
          log.debug("processed:\n\n    {}\n", mutation)
          queued = queued.tail
          return Some(op)
      }
    }
    None
  }

  /**
   * The in-flight message has been persisted, so allow the probe to process it.
   */
  def commit(): Option[UpdateProbeStatus] = {
    // apply the mutation to the probe
    val notifications: Vector[ProbeNotification] = inflight.map(_._1) match {
      case None => Vector.empty
      case Some(event: EventMutation) =>
        applyStatus(event.status)
        parent ! ChildMutates(probeRef, event.status)
        event.notifications
      case Some(command: CommandMutation) =>
        applyStatus(command.status)
        parent ! ChildMutates(probeRef, command.status)
        command.caller ! command.result
        command.notifications
      case Some(config: ConfigMutation) =>
        children = config.children
        policy = config.policy
        applyStatus(config.status)
        parent ! ChildMutates(probeRef, config.status)
        config.notifications
      case Some(deletion: Deletion) =>
        applyStatus(deletion.status)
        parent ! ChildMutates(probeRef, deletion.status)
        deletion.notifications
    }
    // send notifications
    notifications.foreach { notification => services ! notification }
    // done processing inflight
    inflight = None
    // process the next queued message
    process()
  }

  /**
   * The in-flight message has failed to persist, so perform recovery.  Right now, we
   * don't actually retry, we just drop the in-flight message and start processing the
   * next one.
   */
  def recover(): Option[UpdateProbeStatus] = process()

  /**
   * filter out notifications if they don't match the current policy
   */
  private def filterNotifications(notifications: Vector[ProbeNotification]): Vector[ProbeNotification] = notifications.filter {
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
}

sealed trait Mutation {
  val status: ProbeStatus
  val notifications: Vector[ProbeNotification]
}
case class CommandMutation(caller: ActorRef,
                           result: ProbeResult,
                           status: ProbeStatus,
                           notifications: Vector[ProbeNotification]) extends Mutation
case class EventMutation(status: ProbeStatus,
                         notifications: Vector[ProbeNotification]) extends Mutation
case class ConfigMutation(policy: ProbePolicy,
                          children: Set[ProbeRef],
                          metrics: Set[MetricSource],
                          status: ProbeStatus,
                          notifications: Vector[ProbeNotification]) extends Mutation
case class Deletion(status: ProbeStatus,
                    notifications: Vector[ProbeNotification],
                    lsn: Long) extends Mutation

sealed trait QueuedMessage
case class QueuedEvent(event: ProbeEvent, timestamp: DateTime) extends QueuedMessage
case class QueuedCommand(command: ProbeCommand, caller: ActorRef) extends QueuedMessage
case class QueuedChange(change: ChangeProbe, timestamp: DateTime) extends QueuedMessage
case class QueuedRetire(retire: RetireProbe, timestamp: DateTime) extends QueuedMessage
