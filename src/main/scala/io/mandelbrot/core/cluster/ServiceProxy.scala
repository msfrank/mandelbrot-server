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

import io.mandelbrot.core.ServerConfig
import io.mandelbrot.core.system._
import io.mandelbrot.core.history._
import io.mandelbrot.core.notification._
import io.mandelbrot.core.registry._
import io.mandelbrot.core.state._
import io.mandelbrot.core.tracking._

/**
 * ServiceProxy is a router for all service operation messages, responsible for sending
 * operations to the correct service.
 */
class ServiceProxy extends Actor with ActorLogging {

  val settings = ServerConfig(context.system).settings

  val registryService = context.actorOf(RegistryManager.props(), "registry-service")
  val trackingService = context.actorOf(TrackingManager.props(), "tracking-service")
  val historyService = context.actorOf(HistoryManager.props(), "history-service")
  val notificationService = context.actorOf(NotificationManager.props(), "notification-service")
  val stateService = context.actorOf(StateManager.props(), "state-service")

  //
  val keyExtractor: EntityFunctions.KeyExtractor = {
    case op: RegistryServiceCommand => "registry/"
    case op: ProbeOperation => "system/" + op.probeRef.uri.toString
    case op: ProbeSystemOperation => "system/" + op.uri.toString
  }
  val propsCreator: EntityFunctions.PropsCreator = {
    case op: RegistryServiceCommand => RegistryCoordinator.props(registryService)
    case op: ProbeOperation => ProbeSystem.props(services = self)
    case op: ProbeSystemOperation => ProbeSystem.props(services = self)
  }

  val coordinator = if (settings.cluster.enabled)
    context.actorOf(ClusterManager.props(settings.cluster, keyExtractor, propsCreator), "cluster-coordinator")
  else
    context.actorOf(StandaloneCoordinator.props(registryService), "standalone-coordinator")

  def receive = {

    case op: RegistryServiceQuery =>
      registryService forward op

    case op: RegistryServiceOperation =>
      coordinator forward op

    case op: ProbeSystemOperation =>
      coordinator forward op

    case op: ProbeOperation =>
      coordinator forward op

    case op: StateServiceOperation =>
      stateService forward op

    case op: HistoryServiceOperation =>
      historyService forward op

    case op: NotificationServiceOperation =>
      notificationService forward op

    case op: TrackingServiceOperation =>
      trackingService forward op
  }
}

object ServiceProxy {
  def props() =  Props(classOf[ServiceProxy])
}

