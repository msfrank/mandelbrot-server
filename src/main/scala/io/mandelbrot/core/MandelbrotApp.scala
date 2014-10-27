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
import org.slf4j.LoggerFactory
import scala.concurrent.Await

/**
 * application entry point
 */
object MandelbrotApp extends App {
  val log = LoggerFactory.getLogger("io.mandelbrot.core.MandelbrotApp")

  try {
    /* start the actor system */
    val system = ActorSystem("mandelbrot")

    /* load application settings */
    val settings = ServerConfig(system).settings

    /* pre-warm top level services */
    val supervisor = system.actorOf(MandelbrotSupervisor.props(), "supervisor")

    /* shut down cleanly */
    sys.addShutdownHook({
      try {
        Await.result(supervisor.ask(TerminateSupervisor)(Timeout(settings.shutdownTimeout)), settings.shutdownTimeout)
      } catch {
        case ex: Throwable => log.error("application shutdown failed: " + ex.getMessage)
      }
      system.shutdown()
      system.awaitTermination()
    })

  } catch {
    case ex: Throwable =>
      ex.printStackTrace()
      sys.exit(1)
  }
}
