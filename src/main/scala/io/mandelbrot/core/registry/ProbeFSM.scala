package io.mandelbrot.core.registry

import java.util.UUID

import akka.actor.{Actor, Stash, LoggingFSM, ActorRef}
import akka.pattern.ask
import io.mandelbrot.core.{ResourceNotFound, ApiException}
import io.mandelbrot.core.registry.Probe.SendNotifications
import org.joda.time.DateTime
import scala.collection.mutable
import scala.util.{Failure, Success}

import io.mandelbrot.core.notification.{SquelchNotifications, EmitNotifications, EscalateNotifications, Notification}
import io.mandelbrot.core.state.ProbeState

/**
 *
 */
trait ProbeFSM extends LoggingFSM[ProbeFSMState,ProbeFSMData] with Actor with Stash {

  // for ask pattern
  import context.dispatcher
  implicit val timeout: akka.util.Timeout

  // config
  val probeRef: ProbeRef
  val parent: ActorRef
  val probeGeneration: Long
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
  var expiryTimer: Timer
  var alertTimer: Timer

  /**
   * wait for ProbeState from state service.  transition to Scalar or Aggregate state
   * (depending on the policy) if the lsn returned equals the probe generation, otherwise
   * transition directly to Retired.
   */
  when(InitializingProbeFSMState) {

    case Event(Success(ProbeState(status, lsn)), _) =>
      // initialize probe state
      lifecycle = status.lifecycle
      health = status.health
      summary = status.summary
      lastChange = status.lastChange
      lastUpdate = status.lastUpdate
      correlationId = status.correlation
      acknowledgementId = status.acknowledged
      squelch = status.squelched
      // this generation is not current, so switch to retired behavior
      if (lsn > probeGeneration) {
        log.debug("probe {} becomes retired (lsn {})", probeRef, lsn)
        unstashAll()
        goto(RetiredProbeFSMState) using RetiredProbeFSMState()
      }
      // otherwise replay any stashed messages and transition to scalar
      else {
        log.debug("probe {} becomes running (lsn {})", probeRef, lsn)
        unstashAll()
        // start the expiry timer using the joining timeout
        resetExpiryTimer()
        goto(ScalarProbeFSMState) using ScalarProbeFSMState()
      }

    case Event(Failure(failure: ApiException), _) if failure.failure == ResourceNotFound =>
      log.debug("probe {} becomes retired", probeRef)
      unstashAll()
      goto(RetiredProbeFSMState) using RetiredProbeFSMState()

    case Event(Failure(failure: Throwable), _) =>
      throw failure

    case other =>
      stash()
      stay()
  }

  /**
   *
   */
  when (RetiredProbeFSMState) {

    case Event(RetireProbe(lsn), _) =>
      context.stop(self)
      stay()

    // ignore any other message
    case _ =>
      stay()
  }

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
    stateService.ask(ProbeState(status, probeGeneration)).onComplete {
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
case object InitializingProbeFSMState extends ProbeFSMState
case object RetiredProbeFSMState extends ProbeFSMState

trait ProbeFSMData
case class InitializingProbeFSMState() extends ProbeFSMData
case class RetiredProbeFSMState() extends ProbeFSMData

