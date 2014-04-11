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
import scala.concurrent.duration._

import io.mandelbrot.core.metadata.MetadataManager
import io.mandelbrot.core.notification.NotificationManager
import io.mandelbrot.core.registry.ProbeRegistry
import io.mandelbrot.core.http.{HttpSettings, HttpServer}

/**
 * application entry point
 */
object MandelbrotApp extends App {

  /* start the actor system */
  val system = ActorSystem("mandelbrot")

  /* start top level actors */
  val notificationManager = system.actorOf(NotificationManager.props(), "notification-manager")
  val metadataManager = system.actorOf(MetadataManager.props(), "metadata-manager")
  val objectRegistry = system.actorOf(ProbeRegistry.props(metadataManager, notificationManager), "object-registry")
  val httpSettings = new HttpSettings("localhost", 8080, 10, 30.seconds)
  val httpServer = system.actorOf(HttpServer.props(objectRegistry, httpSettings))

  /* shut down cleanly */
  sys.addShutdownHook({
    system.shutdown()
  })
}
