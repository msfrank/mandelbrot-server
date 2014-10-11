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

import java.util.UUID

import akka.actor.{Actor, Stash, LoggingFSM, ActorRef}
import akka.pattern.ask
import akka.pattern.pipe
import io.mandelbrot.core.metrics.MetricsBus
import io.mandelbrot.core.util.Timer
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.Future
import scala.util.{Failure, Success}

import io.mandelbrot.core._
import io.mandelbrot.core.registry._
import io.mandelbrot.core.notification._
import io.mandelbrot.core.state.{InitializeProbeState, ProbeStatusCommitted, ProbeState}
import io.mandelbrot.core.tracking._

/**
 * Base trait for the Probe FSM, containing the initializing and retiring logic,
 * as well as methods common to all Probe behaviors.
 */
trait ProbeOperations extends Actor with ProbeInterface {

  var processor: ProbeBehaviorInterface
  var _lifecycle: ProbeLifecycle
  var _health: ProbeHealth
  var _summary: Option[String]
  var _lastChange: Option[DateTime]
  var _lastUpdate: Option[DateTime]
  var _correlationId: Option[UUID]
  var _acknowledgementId: Option[UUID]
  var _squelch: Boolean

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
   *
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

  /**
   * send the notification if the notification set policy is not specified (meaning
   * send all notifications) or if the policy is specified and this specific notification
   * type is in the notification set.
   */
  def sendNotification(notification: Notification): Unit = notification match {
    case alert: Alert =>
      services.notificationService ! alert
    case _ =>
      if (policy.notifications.isEmpty)
        services.notificationService ! notification
      else if (policy.notifications.get.contains(notification.kind))
        services.notificationService ! notification
  }

  def sendNotifications(notifications: Iterable[Notification]): Unit = {
    println("sendNotifications: " + notifications)
    notifications.foreach(sendNotification)
  }

//  /**
//   *
//   */
//  def acknowledgeProbe(command: AcknowledgeProbe, caller: ActorRef): Unit = correlationId match {
//    case None =>
//      sender() ! ProbeOperationFailed(command, new ApiException(ResourceNotFound))
//    case Some(correlation) if acknowledgementId.isDefined =>
//      sender() ! ProbeOperationFailed(command, new ApiException(Conflict))
//    case Some(correlation) if correlation != command.correlationId =>
//      sender() ! ProbeOperationFailed(command, new ApiException(BadRequest))
//    case Some(correlation) =>
//      val acknowledgement = UUID.randomUUID()
//      val timestamp = DateTime.now(DateTimeZone.UTC)
//      val correlation = correlationId.get
//      _acknowledgementId = Some(acknowledgement)
//      val status = ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
//      val notifications = Vector(NotifyAcknowledged(probeRef, timestamp, correlation, acknowledgement))
//      // update state and send notifications
//      commitStatusAndNotify(status, notifications).flatMap { committed =>
//        // create a ticket to track the acknowledgement
//        services.trackingService.ask(CreateTicket(acknowledgement, timestamp, probeRef, correlation)).map {
//          case result: CreateTicketResult =>
//            AcknowledgeProbeResult(command, acknowledgement)
//          case failure: TrackingServiceOperationFailed =>
//            ProbeOperationFailed(command, failure.failure)
//        }.recover { case ex => ProbeOperationFailed(command, new ApiException(InternalError))}
//      }.recover {
//        case ex =>
//          // FIXME: what is the impact on consistency if commit fails?
//          ProbeOperationFailed(command, ex)
//      }.pipeTo(caller)
//  }
//
//  /**
//   *
//   */
//  def unacknowledgeProbe(command: UnacknowledgeProbe, caller: ActorRef): Unit = acknowledgementId match {
//    case None =>
//      sender() ! ProbeOperationFailed(command, new ApiException(ResourceNotFound))
//    case Some(acknowledgement) if acknowledgement != command.acknowledgementId =>
//      sender() ! ProbeOperationFailed(command, new ApiException(BadRequest))
//    case Some(acknowledgement) =>
//      val timestamp = DateTime.now(DateTimeZone.UTC)
//      val correlation = correlationId.get
//      acknowledgementId = None
//      val status = ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
//      val notifications = Vector(NotifyUnacknowledged(probeRef, timestamp, correlation, acknowledgement))
//      // update state and send notifications
//      commitStatusAndNotify(status, notifications).flatMap { committed =>
//        // close the ticket
//        services.trackingService.ask(CloseTicket(acknowledgement)).map {
//          case result: CloseTicketResult =>
//            UnacknowledgeProbeResult(command, acknowledgement)
//          case failure: TrackingServiceOperationFailed =>
//            ProbeOperationFailed(command, failure.failure)
//        }.recover { case ex => ProbeOperationFailed(command, new ApiException(InternalError))}
//      }.recover {
//        case ex =>
//          // FIXME: what is the impact on consistency if commit fails?
//          ProbeOperationFailed(command, ex)
//      }.pipeTo(sender())
//  }
//
//  /**
//   *
//   */
//  def appendComment(command: AppendProbeWorknote, caller: ActorRef): Unit = acknowledgementId match {
//    case None =>
//      sender() ! ProbeOperationFailed(command, new ApiException(ResourceNotFound))
//    case Some(acknowledgement) if acknowledgement != command.acknowledgementId =>
//      sender() ! ProbeOperationFailed(command, new ApiException(BadRequest))
//    case Some(acknowledgement) =>
//      val timestamp = DateTime.now(DateTimeZone.UTC)
//      services.trackingService.tell(AppendWorknote(acknowledgement, timestamp, command.comment, command.internal.getOrElse(false)), sender())
//  }
//
//  /**
//   *
//   */
//  def squelchProbe(command: SetProbeSquelch, caller: ActorRef): Unit = {
//    if (squelch == command.squelch) {
//      sender() ! ProbeOperationFailed(command, new ApiException(BadRequest))
//    } else {
//      val timestamp = DateTime.now(DateTimeZone.UTC)
//      squelch = command.squelch
//      val status = ProbeStatus(probeRef, timestamp, lifecycle, health, summary, lastUpdate, lastChange, correlationId, acknowledgementId, squelch)
//      val notifications = if (command.squelch) Vector(NotifySquelched(probeRef, timestamp)) else Vector(NotifyUnsquelched(probeRef, timestamp))
//      // update state and send notifications
//      commitStatusAndNotify(status, notifications).map { _ =>
//        SetProbeSquelchResult(command, command.squelch)
//      }.recover {
//        case ex =>
//          // FIXME: what is the impact on consistency if commit fails?
//          ProbeOperationFailed(command, ex)
//      }.pipeTo(caller)
//    }
//  }
}

