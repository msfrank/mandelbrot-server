package io.mandelbrot.core.system

import akka.event.LoggingAdapter
import org.joda.time.{DateTimeZone, DateTime}
import java.util.UUID

import io.mandelbrot.core.model._
import io.mandelbrot.core.util.Timer

/**
 * MutationOps trait encapsulates the mutable state of a probe.  the actual modification
 * of probe state only occurs in the applyStatus method.  MutationOps also implements the
 * ProbeInterface trait, which is a read-only view of probe state that a BehaviorProcessor
 * can access.
 */
trait MutationOps extends ProbeInterface {

  implicit def log: LoggingAdapter

  val expiryTimer: Timer
  val alertTimer: Timer

  def children: Set[ProbeRef]
  def policy: CheckPolicy

  private var _lifecycle: ProbeLifecycle = ProbeInitializing
  private var _health: ProbeHealth = ProbeUnknown
  private var _summary: Option[String] = None
  private var _lastChange: Option[DateTime] = None
  private var _lastUpdate: Option[DateTime] = None
  private var _correlationId: Option[UUID] = None
  private var _acknowledgementId: Option[UUID] = None
  private var _squelch: Boolean = false

  override def lifecycle: ProbeLifecycle = _lifecycle

  override def summary: Option[String] = _summary

  override def health: ProbeHealth = _health

  override def lastUpdate: Option[DateTime] = _lastUpdate

  override def lastChange: Option[DateTime] = _lastChange

  override def correlationId: Option[UUID] = _correlationId

  override def acknowledgementId: Option[UUID] = _acknowledgementId

  override def squelch: Boolean = _squelch

  /**
   * apply the updated status to the probe, and update alert and expiry
   * timers as necessary.
   */
  def applyStatus(status: ProbeStatus): Unit = {
    // we don't alert if lifecycle is not known or synthetic
    if (status.lifecycle != ProbeKnown && status.lifecycle != ProbeSynthetic) {
      alertTimer.stop()
    }
    // if health transitions from unhealthy to healthy, then stop the alert timer
    else if (health != ProbeHealthy && status.health == ProbeHealthy) {
      alertTimer.stop()
    }
    // if health is acknowledged, then stop the alert timer
    else if (status.acknowledged.nonEmpty) {
      alertTimer.stop()
    }
    // if unhealthy and the alert timer is not running, then start the alert timer
    else if (health != ProbeHealthy && !alertTimer.isRunning) {
      alertTimer.start(policy.alertTimeout)
    }
    log.debug("alert timer => {}", alertTimer)
    status.lifecycle match {
      // if lifecycle is initializing or synthetic, then stop the expiry timer
      case ProbeInitializing | ProbeSynthetic =>
        expiryTimer.stop()
      // if lifecycle is joining, start the expiry timer using joining timeout
      case ProbeJoining =>
        expiryTimer.restart(policy.joiningTimeout)
      // if lifecycle is known, start the expiry timer using probe timeout
      case ProbeKnown =>
        expiryTimer.restart(policy.probeTimeout)
      // if lifecycle is retired, start the expiry timer using leaving timeout
      case ProbeRetired =>
        expiryTimer.restart(policy.leavingTimeout)
    }
    log.debug("expiry timer => {}", expiryTimer)
    // modify internal status fields
    _lifecycle = status.lifecycle
    _health = status.health
    _summary = status.summary
    _lastChange = status.lastChange
    _lastUpdate = status.lastUpdate
    _correlationId = status.correlation
    _acknowledgementId = status.acknowledged
    _squelch = status.squelched
    log.debug("applied:\n\n    {}\n", status)
  }

  /* shortcut to get the current time */
  // FIXME: use Timestamp instead
  def now() = DateTime.now(DateTimeZone.UTC)

}
