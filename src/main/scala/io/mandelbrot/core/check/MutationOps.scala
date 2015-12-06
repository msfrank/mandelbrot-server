package io.mandelbrot.core.check

import akka.event.LoggingAdapter
import io.mandelbrot.core.{ResourceNotFound, Conflict, ApiException, BadRequest}
import org.joda.time.{DateTimeZone, DateTime}
import java.util.UUID

import io.mandelbrot.core.model._
import io.mandelbrot.core.util.Timer

import scala.util.{Success, Try, Failure}

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

  /**
   *
   */
  def processStatus(timestamp: DateTime, status: ProcessorStatus): Option[EventEffect] = {
    // update check status based on the processor health
    val nextHealth = status.health
    val nextSummary = status.summary
    val nextLastUpdate = Some(timestamp)
    val nextLastChange = if (status.health != health) Some(timestamp) else None
    val nextCorrelationId = if (health == CheckHealthy && status.health != CheckHealthy && correlationId.isEmpty) Some(UUID.randomUUID()) else None
    val nextAcknowledgementId = if (nextCorrelationId.isEmpty) None else acknowledgementId
    val nextStatus = CheckStatus(generation, timestamp, lifecycle, nextSummary, nextHealth,
      nextLastUpdate, nextLastChange, nextCorrelationId, nextAcknowledgementId, squelch)
    //
    var notifications: Vector[CheckNotification] = Vector.empty
    if (status.health != health) {
      if (nextHealth == CheckHealthy && correlationId.isDefined && acknowledgementId.isDefined)
        notifications = notifications :+ NotifyRecovers(checkRef, timestamp, correlationId.get, acknowledgementId.get)
      else
        notifications = notifications :+ NotifyHealthChanges(checkRef, timestamp, nextCorrelationId, health, nextHealth)
    } else {
      notifications = notifications :+ NotifyHealthUpdates(checkRef, timestamp, nextCorrelationId, nextHealth)
    }
    Some(EventEffect(nextStatus, notifications))
  }

  /**
   *
   */
  def processAcknowledge(command: AcknowledgeCheck): Try[CommandEffect] = {
    correlationId match {
      case None =>
        Failure(ApiException(ResourceNotFound))
      case Some(correlation) if acknowledgementId.isDefined =>
        Failure(ApiException(Conflict))
      case Some(correlation) if correlation != command.correlationId =>
        Failure(ApiException(BadRequest))
      case Some(correlation) =>
        val acknowledgement = UUID.randomUUID()
        val timestamp = DateTime.now(DateTimeZone.UTC)
        val status = getCheckStatus(timestamp).copy(acknowledged = Some(acknowledgement))
        val condition = CheckCondition(generation, timestamp, status.lifecycle, status.summary,
          status.health, status.correlation, status.acknowledged, status.squelched)
        val notifications = Vector(NotifyAcknowledged(checkRef, timestamp, correlation, acknowledgement))
        Success(CommandEffect(AcknowledgeCheckResult(command, condition), status, notifications))
    }
  }

  def processUnacknowledge(command: UnacknowledgeCheck): Try[CommandEffect] = {
    acknowledgementId match {
      case None =>
        Failure(ApiException(ResourceNotFound))
      case Some(acknowledgement) if acknowledgement != command.acknowledgementId =>
        Failure(ApiException(BadRequest))
      case Some(acknowledgement) =>
        val timestamp = DateTime.now(DateTimeZone.UTC)
        val correlation = correlationId.get
        val status = getCheckStatus(timestamp).copy(acknowledged = None)
        val condition = CheckCondition(generation, timestamp, status.lifecycle, status.summary,
          status.health, status.correlation, status.acknowledged, status.squelched)
        val notifications = Vector(NotifyUnacknowledged(checkRef, timestamp, correlation, acknowledgement))
        Success(CommandEffect(UnacknowledgeCheckResult(command, condition), status, notifications))
    }
  }

  def processSetSquelch(command: SetCheckSquelch): Try[CommandEffect] = {
    if (squelch == command.squelch) Failure(ApiException(BadRequest)) else {
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val squelch = command.squelch
      val status = getCheckStatus(timestamp).copy(squelched = squelch)
      val condition = CheckCondition(generation, timestamp, status.lifecycle, status.summary,
        status.health, status.correlation, status.acknowledged, status.squelched)
      val notifications = if (command.squelch) Vector(NotifySquelched(checkRef, timestamp)) else Vector(NotifyUnsquelched(checkRef, timestamp))
      Success(CommandEffect(SetCheckSquelchResult(command, condition), status, notifications))
    }
  }

  def processCommand(command: CheckCommand): Try[CommandEffect] = command match {
    case cmd: AcknowledgeCheck => processAcknowledge(cmd)
    case cmd: UnacknowledgeCheck => processUnacknowledge(cmd)
    case cmd: SetCheckSquelch => processSetSquelch(cmd)
    case _ => throw new IllegalArgumentException()
  }

  /**
   * check lifecycle is leaving and the leaving timeout has expired.  check lifecycle is set to
   * retired, state is updated, and lifecycle-changes notification is sent.  finally, all timers
   * are stopped, then the actor itself is stopped.
   */
  def processRetirement(lsn: Long): Option[EventEffect] = {
    val timestamp = DateTime.now(DateTimeZone.UTC)
    val status = getCheckStatus(timestamp).copy(lifecycle = CheckRetired, lastChange = Some(timestamp), lastUpdate = Some(timestamp))
    val notifications = Vector(NotifyLifecycleChanges(checkRef, timestamp, lifecycle, CheckRetired))
    Some(EventEffect(status, notifications))
  }
}

sealed trait CheckEffect
case class CommandEffect(result: CheckResult,
                         status: CheckStatus,
                         notifications: Vector[CheckNotification]) extends CheckEffect

case class EventEffect(status: CheckStatus,
                       notifications: Vector[CheckNotification]) extends CheckEffect
