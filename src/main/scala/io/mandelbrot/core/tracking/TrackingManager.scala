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

package io.mandelbrot.core.tracking

import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import io.mandelbrot.core.system.ProbeRef
import org.joda.time.DateTime
import java.util.UUID

import io.mandelbrot.core.{ServiceMap, ServiceExtension, ServerConfig}

/**
 *
 */
class TrackingManager extends Actor with ActorLogging {

  // config
  val settings = ServerConfig(context.system).settings.tracking
//  val tracker: ActorRef = {
//    val props = ServiceExtension.makePluginProps(settings.tracker.plugin, settings.tracker.settings)
//    log.info("loading tracker plugin {}", settings.tracker.plugin)
//    context.actorOf(props, "tracker")
//  }

  def receive = {

    case services: ServiceMap =>
      // do nothing

    case command: CreateTicket =>
      log.debug("create ticket acknowledging {} for {}", command.correlation, command.probeRef)
      sender() ! CreateTicketResult(command, command.acknowledgement)

    case command: AppendWorknote =>
      log.debug("append worknote for {}", command.acknowledgement)
      sender() ! AppendWorknoteResult(command, command.acknowledgement)

    case command: ResolveTicket =>
      log.debug("resolve ticket for {}", command.acknowledgement)
      sender() ! ResolveTicketResult(command, command.acknowledgement)
  }
}

object TrackingManager {
  def props() = Props(classOf[TrackingManager])
}

/**
 *
 */
sealed trait TrackingServiceOperation
sealed trait TrackingServiceCommand extends TrackingServiceOperation
sealed trait TrackingServiceQuery extends TrackingServiceOperation
case class TrackingServiceOperationFailed(op: TrackingServiceOperation, failure: Throwable)

case class ListTrackingTickets(last: Option[String], limit: Option[Int]) extends TrackingServiceQuery
case class ListTrackingTicketsResult(op: ListTrackingTickets, tickets: Vector[UUID], last: Option[String])

case class CreateTicket(acknowledgement: UUID, timestamp: DateTime, probeRef: ProbeRef, correlation: UUID) extends TrackingServiceCommand
case class CreateTicketResult(op: CreateTicket, ticket: UUID)

case class AppendWorknote(acknowledgement: UUID, timestamp: DateTime, description: String, internal: Boolean) extends TrackingServiceCommand
case class AppendWorknoteResult(op: AppendWorknote, ticket: UUID)

case class ResolveTicket(acknowledgement: UUID) extends TrackingServiceCommand
case class ResolveTicketResult(op: ResolveTicket, ticket: UUID)

/* marker trait for Tracker implementations */
trait Tracker
