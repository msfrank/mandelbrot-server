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

import akka.actor.{Props, ActorRef, ActorLogging}
import akka.persistence.EventsourcedProcessor
import scala.collection.mutable
import scala.util.{Success, Failure}

import io.mandelbrot.core.{ResourceNotFound, ApiException, ServiceExtension, ServerConfig}
import io.mandelbrot.core.registry._
import io.mandelbrot.core.history.HistoryService

/**
 * the state manager holds the current status of all probes in memory.  if a Searcher
 * plugin is configured, then the status and metadata of each probe is also stored in
 * a format which may be searched.  the exact syntax of a state service query string is
 * dependent on the Searcher implementation.
 */
class StateManager extends EventsourcedProcessor with ActorLogging {
  import StateManager._

  // config
  override def processorId = "state-manager"
  val settings = ServerConfig(context.system).settings.state
  val searcher: ActorRef = {
    val props = ServiceExtension.makePluginProps(settings.searcher.plugin, settings.searcher.settings)
    log.info("loading searcher plugin {}", settings.searcher.plugin)
    context.actorOf(props, "searcher")
  }

  // state
  val probeState = new mutable.HashMap[ProbeRef,ProbeState]()

  // refs
  val historyService = HistoryService(context.system)

  def receiveCommand = {

    /*
     *
     */
    case state: ProbeState =>
      persist(ProbeStatusUpdates(state.status, state.lsn))(updateState)

    case GetProbeState(ref) =>
      probeState.get(ref) match {
        case Some(state) =>
          sender() ! Success(state)
        case None =>
          sender() ! Failure(new ApiException(ResourceNotFound))
      }

    /*
     *
     */
    case command: DeleteProbeState =>
      if (!probeState.contains(command.ref))
        sender() ! StateServiceOperationFailed(command, new ApiException(ResourceNotFound))
      else
        persist(ProbeStatusDeleted(command.ref, command.lastStatus))(updateState)

    /*
     *
     */
    case metadata: ProbeMetadata =>
      searcher ! metadata

    /*
     *
     */
    case query: GetCurrentStatus =>
      val target = query.refspec match {
        case Left(ref) if !probeState.contains(ref) => Set.empty[ProbeRef]
        case Left(ref) => Set(ref)
        case Right(refs) => refs.filter(probeState.contains)
      }
      if (!target.isEmpty) {
        val status = target.map { ref => probeState(ref).status }.toVector
        sender() ! GetCurrentStatusResult(query, status)
      } else
        sender() ! StateServiceOperationFailed(query, new ApiException(ResourceNotFound))

    /*
     *
     */
    case query: QueryProbes =>
      searcher.forward(query)
  }

  def receiveRecover = {
    case event: Event =>
      updateState(event)
  }

  def updateState(event: Event): Unit = event match {

    case ProbeStatusUpdates(status, lsn) =>
      probeState.put(status.probeRef, ProbeState(status, lsn))
      if (!recoveryRunning) {
        searcher ! status
        historyService ! status
      }

    case ProbeStatusDeleted(ref: ProbeRef, lastStatus: Option[ProbeStatus]) =>
      probeState.remove(ref)
      if (!recoveryRunning) {
        // FIXME: delete state from searcher
        //searcher ! DeleteProbeState(ref, lastStatus)
        for (status <- lastStatus)
          historyService ! status
      }
  }
}

object StateManager {
  def props() = Props(classOf[StateManager])
  sealed trait Event
  case class ProbeStatusUpdates(status: ProbeStatus, lsn: Long) extends Event
  case class ProbeStatusDeleted(ref: ProbeRef, lastStatus: Option[ProbeStatus]) extends Event
  case class StateManagerSnapshot(probeStatus: Map[ProbeRef,ProbeState]) extends Serializable
}

case class GetProbeState(ref: ProbeRef)
case class ProbeState(status: ProbeStatus, lsn: Long)

/**
 *
 */
sealed trait StateServiceOperation
sealed trait StateServiceCommand extends StateServiceOperation
sealed trait StateServiceQuery extends StateServiceOperation
case class StateServiceOperationFailed(op: StateServiceOperation, failure: Throwable)

case class GetCurrentStatus(refspec: Either[ProbeRef,Set[ProbeRef]]) extends StateServiceCommand
case class GetCurrentStatusResult(op: GetCurrentStatus, status: Vector[ProbeStatus])

case class DeleteProbeState(ref: ProbeRef, lastStatus: Option[ProbeStatus]) extends StateServiceCommand
case class DeleteProbeStateResult(op: DeleteProbeState, status: ProbeStatus)

case class QueryProbes(query: String, limit: Option[Int]) extends StateServiceQuery
case class QueryprobesResult(op: QueryProbes, refs: Vector[ProbeRef])

/* marker trait for Searcher implementations */
trait Searcher
