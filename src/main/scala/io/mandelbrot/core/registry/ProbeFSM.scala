package io.mandelbrot.core.registry

import java.util.UUID

import akka.actor.{LoggingFSM, ActorRef}
import akka.pattern.ask
import akka.pattern.pipe
import io.mandelbrot.core.registry.Probe.SendNotifications
import org.joda.time.DateTime
import scala.collection.mutable
import scala.util.{Failure, Success}

import io.mandelbrot.core.notification.{SquelchNotifications, EmitNotifications, EscalateNotifications, Notification}
import io.mandelbrot.core.state.ProbeState

/**
 *
 */
trait ProbeFSM extends LoggingFSM[ProbeFSMState,ProbeFSMData] {

  // for ask pattern
  import context.dispatcher
  implicit val timeout: Timeout

  // config
  val probeRef: ProbeRef
  val parent: ActorRef
  val generation: Long
  val stateService: ActorRef
  val notificationService: ActorRef
  val trackingService: ActorRef

  // state
  var children: Set[ProbeRef]
  var policy: ProbePolicy

  var lifecycle: ProbeLifecycle
  var health: ProbeHealth
  var summary: Option[String]
  var lastChange: Option[DateTime]
  var lastUpdate: Option[DateTime]
  var correlationId: Option[UUID]
  var acknowledgementId: Option[UUID]
  var squelch: Boolean

  var notifier: Option[ActorRef]
  var flapQueue: Option[FlapQueue]
  var escalationMap: mutable.HashMap[ProbeRef,Option[ProbeStatus]]
  var expiryTimer: Timer
  var alertTimer: Timer

  /**
   * send the notification if the notification set policy is not specified (meaning
   * send all notifications) or if the policy is specified and this specific notification
   * type is in the notification set.
   */
  def sendNotification(notification: Notification): Unit = notifier.foreach { _notifier =>
    if (policy.notificationPolicy.notifications.isEmpty)
        _notifier ! notification
    else if (policy.notificationPolicy.notifications.get.contains(notification.kind))
        _notifier ! notification
  }

  /**
   * send probe status to the state service, and wait for acknowledgement.  if update is
   * acknowledged then send notifications, otherwise log an error.
   */
  def commitStatusAndNotify(status: ProbeStatus, notifications: Vector[Notification]): Unit = {
    stateService.ask(ProbeState(status, generation)).onComplete {
      case Success(committed) => self ! SendNotifications(notifications)
      // FIXME: what is the impact on consistency if commit fails?
      case Failure(ex) => log.error(ex, "failed to commit probe state")
    }
  }

  /**
   * reset the expiry timer, checking lastTimeout.  this will potentially send
   * a ProbeExpiryTimeout message if the timeout from a new policy is smaller than
   * the old policy.
   */
  def resetExpiryTimer(): Unit = {
    lifecycle match {
      case ProbeJoining =>
        expiryTimer.reset(policy.joiningTimeout)
      case ProbeKnown =>
        expiryTimer.reset(policy.probeTimeout)
      case ProbeRetired =>
        throw new Exception("resetting expiry timer for retired probe")
    }
  }

  /**
   * restart the expiry timer, but don't check lastTimeout when re-arming, otherwise
   * we may get duplicate ProbeExpiryTimeout messages.
   */
  def restartExpiryTimer(): Unit = {
    lifecycle match {
      case ProbeJoining =>
        expiryTimer.restart(policy.joiningTimeout)
      case ProbeKnown =>
        expiryTimer.restart(policy.probeTimeout)
      case ProbeRetired =>
        throw new Exception("restarting expiry timer for retired probe")
    }
  }

  /**
   * update state based on the new policy.
   */
  def applyPolicy(newPolicy: ProbePolicy): Unit = {
    log.debug("probe {} updates configuration: {}", probeRef, newPolicy)
    newPolicy.notificationPolicy.behavior match {
      case EscalateNotifications =>
        notifier = Some(parent)
      case EmitNotifications =>
        notifier = Some(notificationService)
      case SquelchNotifications =>
        notifier = None
    }
    policy = newPolicy
  }
}

trait ProbeFSMState
trait ProbeFSMData

