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

package io.mandelbrot.core.http

import akka.actor.{Props, Actor, ActorLogging}
import akka.io.IO
import akka.util.Timeout
import io.mandelbrot.core.registry.RegistryService
import io.mandelbrot.core.state.StateService
import io.mandelbrot.core.messagestream.MessageStream
import io.mandelbrot.core.history.HistoryService

/**
 * HttpServer is responsible for listening on the HTTP port, accepting connections,
 * and handing them over to the ApiService for processing.
 */
class HttpServer(val settings: HttpSettings) extends Actor with ApiService with ActorLogging {
  import spray.can.Http

  implicit val system = context.system
  implicit val dispatcher = context.dispatcher

  val actorRefFactory = context
  val timeout: Timeout = settings.requestTimeout

  val registryService = RegistryService(system)
  val stateService = StateService(system)
  val historyService = HistoryService(system)
  val messageStream = MessageStream(system)

  override def preStart() {
    IO(Http) ! Http.Bind(self, settings.interface, port = settings.port, backlog = settings.backlog)
    log.debug("binding to %s:%d with backlog %d".format(settings.interface, settings.port, settings.backlog))
  }

  def receive = runRoute(routes) orElse {
    case bound: Http.Bound => log.debug("bound HTTP listener to {}", bound.localAddress)
  }
}

object HttpServer {
  def props(settings: HttpSettings) = Props(classOf[HttpServer], settings)
}

