package io.mandelbrot.core.check

import akka.event.LoggingAdapter
import org.joda.time.{DateTimeZone, DateTime}
import java.util.UUID

import io.mandelbrot.core.model._
import io.mandelbrot.core.util.Timer

/**
 * MutationOps trait encapsulates the mutable state of a check.  the actual modification
 * of check state only occurs in the updateStatus method.  MutationOps also implements the
 * AccessorOps trait, which is a read-only view of check state that a BehaviorProcessor
 * can access.
 */
trait MutationOps extends AccessorOps {

  implicit def log: LoggingAdapter

  def generation: Long
  def children: Set[CheckRef]
  def policy: CheckPolicy

  private var _lifecycle: CheckLifecycle = CheckInitializing
  private var _health: CheckHealth = CheckUnknown
  private var _summary: Option[String] = None
  private var _lastChange: Option[DateTime] = None
  private var _lastUpdate: Option[DateTime] = None
  private var _correlationId: Option[UUID] = None
  private var _acknowledgementId: Option[UUID] = None
  private var _squelch: Boolean = false

  override def lifecycle: CheckLifecycle = _lifecycle

  override def summary: Option[String] = _summary

  override def health: CheckHealth = _health

  override def lastUpdate: Option[DateTime] = _lastUpdate

  override def lastChange: Option[DateTime] = _lastChange

  override def correlationId: Option[UUID] = _correlationId

  override def acknowledgementId: Option[UUID] = _acknowledgementId

  override def squelch: Boolean = _squelch

  /**
   * modify internal status fields using the specified check status.
   */
  def applyStatus(status: CheckStatus): Unit = {
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
