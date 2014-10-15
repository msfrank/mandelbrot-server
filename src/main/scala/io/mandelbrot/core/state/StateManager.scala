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

import akka.actor.{Cancellable, Props, ActorRef, ActorLogging}
import akka.pattern.ask
import akka.pattern.pipe
import akka.persistence._
import akka.util.Timeout
import io.mandelbrot.core.system.ProbeRef
import org.joda.time.DateTime
import scala.concurrent.duration._
import scala.collection.mutable
import scala.util.{Try, Success, Failure}

import io.mandelbrot.core._
import io.mandelbrot.core.system._

/**
 * the state manager holds the current status of all probes in memory.  if a Searcher
 * plugin is configured, then the status and metadata of each probe is also stored in
 * a format which may be searched.  the exact syntax of a state service query string is
 * dependent on the Searcher implementation.
 */
class StateManager extends PersistentActor with ActorLogging {
  import StateManager._
  import context.dispatcher

  // config
  override def persistenceId = "state-manager"
  val settings = ServerConfig(context.system).settings.state
  val searcher: ActorRef = {
    val props = ServiceExtension.makePluginProps(settings.searcher.plugin, settings.searcher.settings)
    log.info("loading searcher plugin {}", settings.searcher.plugin)
    context.actorOf(props, "searcher")
  }
  implicit val timeout = Timeout(5.seconds)   // TODO: pull this from settings

  // state
  var historyService = ActorRef.noSender
  val probeState = new mutable.HashMap[ProbeRef,ProbeState]()
  var currentLsn = Long.MinValue
  var snapshotCancellable: Option[Cancellable] = None

  override def preStart(): Unit = {
    super.preStart()
    // schedule regular snapshots
    snapshotCancellable = Some(context.system.scheduler.schedule(settings.snapshotInitialDelay, settings.snapshotInterval, self, TakeSnapshot))
    log.debug("scheduling {} snapshots every {} with initial delay of {}",
      persistenceId, settings.snapshotInterval.toString(), settings.snapshotInitialDelay.toString())
  }

  override def postStop(): Unit = {
    for (cancellable <- snapshotCancellable)
      cancellable.cancel()
    super.postStop()
  }

  def receiveCommand = {

    case services: ServiceMap =>
      historyService = services.historyService

    /* */
    case op: InitializeProbeState =>
      probeState.get(op.ref) match {
        case Some(state) =>
          sender() ! state
        case None if op.lsn >= currentLsn =>
          persist(op)(updateState)
        case None =>
          sender() ! StateServiceOperationFailed(op, new ApiException(ResourceNotFound))
      }

    /* update current status for ref */
    case op: UpdateProbeState =>
      persist(op)(updateState)

    /* update current metadata for ref */
    case metadata: ProbeMetadata =>
      searcher ! metadata

    /* */
    case op: DeleteProbeState =>
      if (probeState.contains(op.ref)) {
        persist(op)(updateState)
      }

    /* get the current status for each matching ref */
    case query: GetCurrentStatus =>
      val target = query.refspec match {
        case Left(ref) if !probeState.contains(ref) => Set.empty[ProbeRef]
        case Left(ref) => Set(ref)
        case Right(refs) => refs.filter(probeState.contains)
      }
      if (target.nonEmpty) {
        val status = target.map { ref => probeState(ref).status }.toVector
        sender() ! GetCurrentStatusResult(query, status)
      } else
        sender() ! StateServiceOperationFailed(query, new ApiException(ResourceNotFound))

    /* search for matching refs, then return the current status for each */
    case query: SearchCurrentStatus =>
      val caller = sender()
      searcher.ask(QueryProbes(query.query, query.limit)).map {
        case Success(results: QueryProbesResult) => OpCallerResults(query, caller, Success(results))
        case Failure(ex: ApiException) => OpCallerResults(query, caller, Failure(ex))
        case Failure(ex: Throwable) => OpCallerResults(query, caller, Failure(new ApiException(InternalError)))
      }.pipeTo(self)

    case OpCallerResults(op, caller, results) =>
      results match {
        case Success(QueryProbesResult(_, refs)) =>
          val state = refs.filter(probeState.contains).map(probeState.apply)
          val status = state.map { s => s.status.probeRef -> s.status }.toMap
          caller ! SearchCurrentStatusResult(op, status)
        case Failure(failure) =>
          StateServiceOperationFailed(op, failure)
      }

    /* forward probe queries to the searcher */
    case query: QueryProbes =>
      searcher.forward(query)

    /* */
    case TakeSnapshot =>
      log.debug("snapshotting state-manager, last sequence number is {}", lastSequenceNr)
      saveSnapshot(StateManagerSnapshot(currentLsn, probeState.toMap))

    case SaveSnapshotSuccess(metadata) =>
      log.debug("saved snapshot successfully: {}", metadata)

    case SaveSnapshotFailure(metadata, cause) =>
      log.warning("failed to save snapshot {}: {}", metadata, cause.getMessage)
  }

  def receiveRecover = {

    case op: StateServiceCommand =>
      updateState(op)

    /* recreate probe state from snapshot */
    case SnapshotOffer(metadata, snapshot: StateManagerSnapshot) =>
      log.debug("loading snapshot of {} using offer {}", persistenceId, metadata)
      snapshot.probeState.foreach { case (ref,state) =>
        probeState.put(ref, state)
      }
      currentLsn = snapshot.currentLsn
      log.debug("resetting current lsn to {}", currentLsn)
  }

  def updateState(op: StateServiceCommand) = op match {

    case command @ InitializeProbeState(ref, timestamp, lsn) =>
      log.debug("status initializes for {} (lsn {})", ref, lsn)
      val status = ProbeStatus(ref, timestamp, ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
      val state = ProbeState(status, lsn)
      probeState.put(ref, state)
      currentLsn = lsn
      if (!recoveryRunning)
        sender() ! InitializeProbeStateResult(command, status, lsn)

    case command @ UpdateProbeState(ref, status, lsn) =>
      log.debug("status updates for {} (lsn {})", status.probeRef, lsn)
      probeState.put(status.probeRef, ProbeState(status, lsn))
      if (lsn > currentLsn)
        currentLsn = lsn
      if (!recoveryRunning) {
        sender() ! UpdateProbeStateResult(command)
        searcher ! status
        historyService ! status
      }

    case command @ DeleteProbeState(ref: ProbeRef, lastStatus: Option[ProbeStatus], lsn) =>
      log.debug("status deleted for {} (lsn {})", ref, lsn)
      probeState.remove(ref)
      if (lsn > currentLsn)
        currentLsn = lsn
      if (!recoveryRunning) {
        // FIXME: delete state from searcher
        //searcher ! DeleteProbeState(ref, lastStatus)
        for (status <- lastStatus) {
          sender() ! DeleteProbeStateResult(command)
          historyService ! status
        }
      }
  }
}

object StateManager {
  def props() = Props(classOf[StateManager])

  case class OpCallerResults(op: SearchCurrentStatus, caller: ActorRef, results: Try[QueryProbesResult])
  case class StateManagerSnapshot(currentLsn: Long, probeState: Map[ProbeRef,ProbeState])
  case object TakeSnapshot
}

case class ProbeState(status: ProbeStatus, lsn: Long)

/**
 *
 */
sealed trait StateServiceOperation
sealed trait StateServiceCommand extends StateServiceOperation
sealed trait StateServiceQuery extends StateServiceOperation
case class StateServiceOperationFailed(op: StateServiceOperation, failure: Throwable)

case class InitializeProbeState(ref: ProbeRef, timestamp: DateTime, lsn: Long) extends StateServiceCommand
case class InitializeProbeStateResult(op: InitializeProbeState, status: ProbeStatus, lsn: Long)

case class UpdateProbeState(ref: ProbeRef, status: ProbeStatus, lsn: Long) extends StateServiceCommand
case class UpdateProbeStateResult(op: UpdateProbeState)

case class DeleteProbeState(ref: ProbeRef, lastStatus: Option[ProbeStatus], lsn: Long) extends StateServiceCommand
case class DeleteProbeStateResult(op: DeleteProbeState)

case class GetCurrentStatus(refspec: Either[ProbeRef,Set[ProbeRef]]) extends StateServiceQuery
case class GetCurrentStatusResult(op: GetCurrentStatus, status: Vector[ProbeStatus])

case class SearchCurrentStatus(query: String, limit: Option[Int]) extends StateServiceQuery
case class SearchCurrentStatusResult(op: SearchCurrentStatus, status: Map[ProbeRef,ProbeStatus])

case class QueryProbes(query: String, limit: Option[Int]) extends StateServiceQuery
case class QueryProbesResult(op: QueryProbes, refs: Vector[ProbeRef])

/* marker trait for Searcher implementations */
trait Searcher
