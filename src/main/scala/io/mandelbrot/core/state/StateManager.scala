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

package io.mandelbrot.core.state

import akka.actor.{Props, ActorRef, ActorLogging, Actor}

import io.mandelbrot.core.registry._
import io.mandelbrot.core.{ServiceExtension, ServerConfig}
import io.mandelbrot.core.history.HistoryService
import io.mandelbrot.core.registry.ProbeStatus

/**
 *
 */
class StateManager extends Actor with ActorLogging {

  // config
  val settings = ServerConfig(context.system).settings.state
  val searcher: ActorRef = {
    val props = ServiceExtension.makePluginProps(settings.searcher.plugin, settings.searcher.settings)
    log.info("loading searcher plugin {}", settings.searcher.plugin)
    context.actorOf(props, "searcher")
  }

  val historyService = HistoryService(context.system)

  def receive = {

    case status: ProbeStatus =>
      searcher ! status
      historyService ! status

    case metadata: ProbeMetadata =>
      searcher ! metadata

    case query: QueryProbes =>
      searcher ! query
  }
}

object StateManager {
  def props() = Props(classOf[StateManager])
}

/**
 *
 */
sealed trait StateServiceOperation
sealed trait StateServiceCommand extends StateServiceOperation
sealed trait StateServiceQuery extends StateServiceOperation
case class StateServiceOperationFailed(op: StateServiceOperation, failure: Throwable)

case class QueryProbes(query: String, limit: Option[Int]) extends StateServiceQuery
case class QueryprobesResult(op: QueryProbes, refs: Vector[ProbeRef])

/* marker trait for Searcher implementations */
trait Searcher
