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

import akka.actor._
import akka.util.ByteString
import io.mandelbrot.core.system.ProbeRef
import org.joda.time.DateTime

import io.mandelbrot.core._
import io.mandelbrot.core.system._

/**
 *
 */
class StateManager(settings: StateSettings) extends Actor with ActorLogging {

  // config
  val persister: ActorRef = {
    val props = ServiceExtension.makePluginProps(settings.persister.plugin, settings.persister.settings)
    log.info("loading persister plugin {}", settings.persister.plugin)
    context.actorOf(props, "persister")
  }

  def receive = {

    case op: InitializeProbeState =>
      persister forward op
      
    case op: UpdateProbeState =>
      persister forward op
      
    case op: DeleteProbeState =>
      persister forward op

    case op: GetProbeState =>
      persister forward op

  }
}

object StateManager {
  def props(settings: StateSettings) = Props(classOf[StateManager], settings)
}

case class ProbeState(status: ProbeStatus, lsn: Long, context: Option[ByteString])

/**
 *
 */
sealed trait StateServiceOperation extends ServiceOperation
sealed trait StateServiceCommand extends ServiceCommand with StateServiceOperation
sealed trait StateServiceQuery extends ServiceQuery with StateServiceOperation
case class StateServiceOperationFailed(op: StateServiceOperation, failure: Throwable) extends ServiceOperationFailed

case class InitializeProbeState(ref: ProbeRef, timestamp: DateTime, lsn: Long) extends StateServiceCommand
case class InitializeProbeStateResult(op: InitializeProbeState, status: ProbeStatus, lsn: Long)

case class UpdateProbeState(ref: ProbeRef, status: ProbeStatus, lsn: Long) extends StateServiceCommand
case class UpdateProbeStateResult(op: UpdateProbeState)

case class DeleteProbeState(ref: ProbeRef, lastStatus: Option[ProbeStatus], lsn: Long) extends StateServiceCommand
case class DeleteProbeStateResult(op: DeleteProbeState)

case class GetProbeState(probeRef: ProbeRef) extends StateServiceQuery
case class GetProbeStateResult(op: GetProbeState, status: ProbeStatus, lsn: Long)

/* marker trait for Persister implementations */
trait Persister

/* marker trait for Searcher implementations */
trait Searcher
