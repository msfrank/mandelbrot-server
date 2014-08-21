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

package io.mandelbrot.core

import akka.actor._

import io.mandelbrot.core.history.HistoryManager
import io.mandelbrot.core.http.HttpServer
import io.mandelbrot.core.notification.NotificationManager
import io.mandelbrot.core.registry.RegistryManager
import io.mandelbrot.core.state.StateManager
import io.mandelbrot.core.tracking.TrackingManager

/**
 *
 */
class MandelbrotSupervisor extends Actor with ActorLogging {

  val settings = ServerConfig(context.system).settings
  var alive = Set.empty[ActorRef]
  var requestor = ActorRef.noSender

  val services = {
    val registryService = context.actorOf(RegistryManager.props(), "registry-service")
    val trackingService = context.actorOf(TrackingManager.props(), "tracking-service")
    val historyService = context.actorOf(HistoryManager.props(), "history-service")
    val notificationService = context.actorOf(NotificationManager.props(), "notification-service")
    val stateService = context.actorOf(StateManager.props(), "state-service")
    val httpServer = context.actorOf(HttpServer.props(), "http-service")
    alive = Set(trackingService, historyService, notificationService, stateService, registryService, httpServer)
    ServiceMap(registryService, trackingService, historyService, notificationService, stateService, httpServer)
  }

  // monitor lifecycle for all specified actors
  alive.foreach(context.watch)

  // send service map to all top-level services
  alive.foreach(_ ! services)

  def receive = {

    case TerminateSupervisor =>
      if (requestor == ActorRef.noSender) {
        log.debug("shutting down application services")
        requestor = sender()
        alive.foreach { _ ! PoisonPill }
      } else log.debug("ignoring spurious termination message from {}", sender().path)

    case Terminated(actor) =>
      alive = alive - actor
      if (alive.isEmpty) {
        log.debug("application service shutdown complete")
        requestor ! TerminationComplete
      }
  }
}

object MandelbrotSupervisor {
  def props() = Props(classOf[MandelbrotSupervisor])
}

case class ServiceMap(registryService: ActorRef,
                      trackingService: ActorRef,
                      historyService: ActorRef,
                      notificationService: ActorRef,
                      stateService: ActorRef,
                      httpServer: ActorRef)

case object TerminateSupervisor
case object TerminationComplete
