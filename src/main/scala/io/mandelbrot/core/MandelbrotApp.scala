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

import akka.actor.ActorSystem

import io.mandelbrot.core.metadata.MetadataManager
import io.mandelbrot.core.notification.NotificationManager
import io.mandelbrot.core.registry.ProbeRegistry
import io.mandelbrot.core.http.HttpServer
import io.mandelbrot.core.messagestream.MessageStreamService

/**
 * application entry point
 */
object MandelbrotApp extends App {

  /* start the actor system */
  val system = ActorSystem("mandelbrot")
  val settings = ServerConfig(system).settings

  /* start top level services */
  val notificationService = system.actorOf(NotificationManager.props(), "notification-service")
  val metadataService = system.actorOf(MetadataManager.props(), "metadata-service")
  val registryService = system.actorOf(ProbeRegistry.props(metadataService, notificationService), "registry-service")

  /* if message stream is configured, then start the MessageStreamService actor */
  val messageStreamService = settings.messageStream match {
    case Some(messageStreamSettings) =>
      Some(system.actorOf(MessageStreamService.props(messageStreamSettings), "message-service"))
    case None => None
  }

  /* if http server is configured, then start the HttpServer actor */
  val httpServer = settings.http match {
    case Some(httpSettings) =>
      Some(system.actorOf(HttpServer.props(registryService, httpSettings), "http-service"))
    case None => None
  }

  /* shut down cleanly */
  sys.addShutdownHook({
    system.shutdown()
  })
}
