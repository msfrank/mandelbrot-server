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

import io.mandelbrot.core.cluster.ServiceProxy
import io.mandelbrot.core.http.HttpServer

/**
 *
 */
class MandelbrotSupervisor extends Actor with ActorLogging {

  // config
  val settings = ServerConfig(context.system).settings

  // state
  var alive = Set.empty[ActorRef]
  var requestor = ActorRef.noSender

  // val coordinator = system.actorOf(ClusterSingletonManager.props())
  val serviceProxy = context.actorOf(ServiceProxy.props(None), "service-proxy")
  val httpServer = context.actorOf(HttpServer.props(serviceProxy), "http-service")

  alive = Set(httpServer, serviceProxy)

  // monitor lifecycle for all specified actors
  alive.foreach(context.watch)

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

case object TerminateSupervisor
case object TerminationComplete
