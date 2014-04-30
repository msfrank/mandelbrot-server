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
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Await

import io.mandelbrot.core.state.StateService
import io.mandelbrot.core.notification.NotificationService
import io.mandelbrot.core.registry.RegistryService
import io.mandelbrot.core.http.HttpServer
import io.mandelbrot.core.history.HistoryService

/**
 * application entry point
 */
object MandelbrotApp extends App {

  /* start the actor system */
  val system = ActorSystem("mandelbrot")
  val settings = ServerConfig(system).settings

  /* pre-warm top level services */
  val historyService = HistoryService(system)
  val notificationService = NotificationService(system)
  val stateService = StateService(system)
  val registryService = RegistryService(system)

  /* if http server is configured, then start the HttpServer actor */
  val httpServer = settings.http match {
    case Some(httpSettings) =>
      Some(system.actorOf(HttpServer.props(httpSettings), "http-service"))
    case None => None
  }

  /* */
  val watched = Vector(historyService, notificationService, stateService, registryService) ++ httpServer.toVector
  val terminator = system.actorOf(Terminator.props(watched))
  val timeout = 30.seconds

  /* shut down cleanly */
  sys.addShutdownHook({
    Await.result(terminator.ask(TerminateApplication)(Timeout(timeout)), timeout)
    system.shutdown()
    system.awaitTermination()
  })
}

/**
 * responsible for terminating the specified actors, waiting for the termination
 * message from each, then returning success to the caller.
 */
class Terminator(actors: Vector[ActorRef]) extends Actor with ActorLogging {

  // monitor lifecycle for all specified actors
  actors.foreach(context.watch)

  // state
  var alive = actors.toSet
  var requestor = ActorRef.noSender

  def receive = {
    case TerminateApplication if requestor == ActorRef.noSender =>
      log.debug("shutting down application services")
      requestor = sender()
      alive.foreach { _ ! PoisonPill }

    case Terminated(actor) =>
      alive = alive - actor
      if (alive.isEmpty) {
        log.debug("application service shutdown complete")
        requestor ! TerminationComplete
      }
  }
}

object Terminator {
  def props(actors: Vector[ActorRef]) = Props(classOf[Terminator], actors)
}

case object TerminateApplication
case object TerminationComplete
