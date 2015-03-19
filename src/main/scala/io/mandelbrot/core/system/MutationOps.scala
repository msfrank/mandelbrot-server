package io.mandelbrot.core.system

import org.joda.time.{DateTimeZone, DateTime}
import java.util.UUID

import io.mandelbrot.core.model._
import io.mandelbrot.core.util.Timer

/**
 *
 */
trait MutationOps extends ProbeInterface {

  val expiryTimer: Timer
  val alertTimer: Timer

  def children: Set[ProbeRef]
  def policy: ProbePolicy

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

  def applyExpiry(status: ProbeStatus): Unit = {
    lifecycle match {
      case ProbeInitializing => // ignore
      case ProbeSynthetic => // ignore
      case ProbeJoining =>
        expiryTimer.restart(policy.joiningTimeout)
      case ProbeKnown =>
        expiryTimer.restart(policy.probeTimeout)
      case ProbeRetired =>
        expiryTimer.restart(policy.leavingTimeout)
    }
    alertTimer.start(policy.alertTimeout)
    applyStatus(status)
  }

  def applyAlert(status: ProbeStatus): Unit = {
    alertTimer.restart(policy.alertTimeout)
    applyStatus(status)
  }

  def applyStatus(status: ProbeStatus): Unit = {
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
  def now() = DateTime.now(DateTimeZone.UTC)

}
