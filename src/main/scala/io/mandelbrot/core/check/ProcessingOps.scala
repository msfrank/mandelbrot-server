package io.mandelbrot.core.check

import akka.actor.{Actor, ActorRef}
import akka.event.LoggingAdapter
import io.mandelbrot.core.agent.RetireCheck
import io.mandelbrot.core.state.UpdateStatus
import org.joda.time.DateTime
import scala.util.{Failure, Success}

import io.mandelbrot.core.model._

/**
 *
 */
trait ProcessingOps extends Actor with MutationOps {

  implicit def log: LoggingAdapter

  val generation: Long
  val services: ActorRef

  var checkType: String
  var policy: CheckPolicy
  var processor: BehaviorProcessor
  var children: Set[CheckRef]
  var lastCommitted: Option[DateTime]

  private var inflight: Option[(Mutation,UpdateStatus)] = None
  private var queued: Vector[QueuedMessage] = Vector.empty

  /**
   * returns true if no processing is in-flight, otherwise false.
   */
  def idle: Boolean = inflight.isEmpty

  /**
   * if the message queue is empty then immediately start processing the message,
   * otherwise append the message to the queue.
   */
  def enqueue(message: QueuedMessage): Option[UpdateStatus] = {
    queued = queued :+ message
    log.debug("enqueued:\n\n    {}\n", message)
    if (inflight.isEmpty) process() else None
  }

  /**
   * process the current message from the queue.  if processing results in a mutation,
   * then return the mutation, otherwise loop until a mutation is generated or the queue
   * is exhausted.
   */
  def process(): Option[UpdateStatus] = {
    // consume queued messages until we find one to process
    while (queued.nonEmpty) {
      // mutation will contain Some(result) from message processing, or None
      val maybeMutation: Option[Mutation] = queued.head match {

        // process the CheckCommand
        case QueuedCommand(command, caller) =>
          processCommand(command) match {
            case Success(effect) =>
              Some(CommandMutation(caller, effect.result, effect.status, effect.notifications))
            case Failure(ex) =>
              caller ! CheckOperationFailed(command, ex)
              None
          }

        // the check is retiring
        case QueuedRetire(retire, timestamp) =>
          processRetirement(retire.lsn).map { effect =>
            Deletion(effect.status, effect.notifications, retire.lsn)
          }
      }

      maybeMutation match {
        case None =>
          queued = queued.tail
        case Some(mutation: Mutation) =>
          val op = UpdateStatus(checkRef, mutation.status, mutation.notifications, commitEpoch = true)
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
   * The in-flight message has been persisted, so allow the check to process it.
   */
  def commit(): Option[UpdateStatus] = {
    // apply the mutation to the check
    inflight.map(_._1) match {

      case None => Vector.empty

      case Some(event: EventMutation) =>
        applyStatus(event.status)
        parent ! ChildMutates(checkRef, event.status)
        notify(event.notifications)

      case Some(command: CommandMutation) =>
        applyStatus(command.status)
        parent ! ChildMutates(checkRef, command.status)
        command.caller ! command.result
        notify(command.notifications)

      case Some(deletion: Deletion) =>
        applyStatus(deletion.status)
        parent ! ChildMutates(checkRef, deletion.status)
        notify(deletion.notifications)
    }
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
  def recover(): Option[UpdateStatus] = process()

  /**
   * send notifications if they match the current policy
   */
  def notify(notifications: Vector[CheckNotification]): Unit = {
    notifications.filter {
      // always allow alerts
      case alert: Alert => true
      // if there is no explicit policy, or the kind matches the current policy, then allow
      case notification: CheckNotification =>
        policy.notifications match {
          case None => true
          case Some(kind) if kind.contains(notification.kind) => true
          case _ => false
        }
      // drop anything else
      case _ => false
    }.foreach(notification => services ! notification)
  }
}

sealed trait Mutation {
  val status: CheckStatus
  val notifications: Vector[CheckNotification]
}
case class CommandMutation(caller: ActorRef,
                           result: CheckResult,
                           status: CheckStatus,
                           notifications: Vector[CheckNotification]) extends Mutation
case class EventMutation(status: CheckStatus,
                         notifications: Vector[CheckNotification]) extends Mutation
case class Deletion(status: CheckStatus,
                    notifications: Vector[CheckNotification],
                    lsn: Long) extends Mutation

sealed trait QueuedMessage
case class QueuedObservation(probeId: ProbeId, observation: Observation) extends QueuedMessage
case class QueuedEvent(event: CheckEvent, timestamp: DateTime) extends QueuedMessage
case class QueuedCommand(command: CheckCommand, caller: ActorRef) extends QueuedMessage
case class QueuedRetire(retire: RetireCheck, timestamp: DateTime) extends QueuedMessage
