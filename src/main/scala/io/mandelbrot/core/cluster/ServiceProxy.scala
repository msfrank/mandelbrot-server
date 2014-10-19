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

package io.mandelbrot.core.cluster

import akka.actor._
import scala.concurrent.duration._

import io.mandelbrot.core.history.{HistoryManager, HistoryServiceOperationFailed, HistoryServiceOperation}
import io.mandelbrot.core.notification.{NotificationManager, NotificationManagerOperationFailed, NotificationManagerOperation}
import io.mandelbrot.core.registry.{RegistryManager, ProbeRegistryOperationFailed, ProbeRegistryOperation}
import io.mandelbrot.core.state.{StateManager, StateServiceOperationFailed, StateServiceOperation}
import io.mandelbrot.core.tracking.{TrackingManager, TrackingServiceOperationFailed, TrackingServiceOperation}
import io.mandelbrot.core.{RetryLater, ApiException}

/**
 * ServiceProxy is a router for all service operation messages, responsible for sending
 * operations to the correct service.  ServiceProxy is also a circuit breaker; as long as
 * LeaseRenewed messages continue to arrive at regular intervals the breaker remains open,
 * but if the renewals stop then all service operations will return RetryLater.
 */
class ServiceProxy extends Actor with ActorLogging {

  // state
  var lease: Long = 0

  val registryService = context.actorOf(RegistryManager.props(), "registry-service")
  val trackingService = context.actorOf(TrackingManager.props(), "tracking-service")
  val historyService = context.actorOf(HistoryManager.props(), "history-service")
  val notificationService = context.actorOf(NotificationManager.props(), "notification-service")
  val stateService = context.actorOf(StateManager.props(), "state-service")

  def receive = {

    case LeaseRenewed(length) =>
      lease = System.nanoTime() + length.toNanos

    case op: ProbeRegistryOperation =>
      if (System.nanoTime() < lease)
        registryService.forward(op)
      else
        sender() ! ProbeRegistryOperationFailed(op, new ApiException(RetryLater))

    case op: TrackingServiceOperation =>
      if (System.nanoTime() < lease)
        trackingService.forward(op)
      else
        sender() ! TrackingServiceOperationFailed(op, new ApiException(RetryLater))

    case op: HistoryServiceOperation =>
      if (System.nanoTime() < lease)
        historyService.forward(op)
      else
        sender() ! HistoryServiceOperationFailed(op, new ApiException(RetryLater))

    case op: NotificationManagerOperation =>
      if (System.nanoTime() < lease)
        notificationService.forward(op)
      else
        sender() ! NotificationManagerOperationFailed(op, new ApiException(RetryLater))

    case op: StateServiceOperation =>
      if (System.nanoTime() < lease)
        stateService.forward(op)
      else
        sender() ! StateServiceOperationFailed(op, new ApiException(RetryLater))
  }
}

object ServiceProxy {
  def props() =  Props(classOf[ServiceProxy])
}

case class LeaseRenewed(length: FiniteDuration)
