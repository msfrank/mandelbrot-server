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

package io.mandelbrot.core.ingest

import akka.actor._
import io.mandelbrot.core.util.Timestamp
import org.joda.time.DateTime
import java.util.UUID

import io.mandelbrot.core._
import io.mandelbrot.core.model._

/**
 *
 */
class IngestManager(settings: IngestSettings, clusterEnabled: Boolean) extends Actor with ActorLogging {

  // config
  val impl: ActorRef = context.actorOf(settings.props, "ingest-impl")

  def receive = {

    /* append observation to the specified probe */
    case op: AppendObservation =>
      // FIXME: ensure that observation timestamp is within bounds
      impl forward op

    /* get observations from the specified partition */
    case op: GetObservations =>
      impl forward op

    /* list all partitions */
    case op: ListPartitions =>
      impl forward op

    /* get the current iterator token for the specified partition */
    case op: GetCheckpoint =>
      impl forward op

    /* set the iterator token for the specified partition */
    case op: PutCheckpoint =>
      impl forward op

    case unhandled =>
      log.error("dropping unhandled message {}", unhandled)
  }
}

object IngestManager {
  def props(settings: IngestSettings, clusterEnabled: Boolean) = {
    Props(classOf[IngestManager], settings, clusterEnabled)
  }
}

/**
 *
 */
sealed trait IngestServiceOperation extends ServiceOperation
sealed trait IngestServiceCommand extends ServiceCommand with IngestServiceOperation
sealed trait IngestServiceQuery extends ServiceQuery with IngestServiceOperation
sealed trait IngestServiceResult
case class IngestServiceOperationFailed(op: IngestServiceOperation, failure: Throwable) extends ServiceOperationFailed

case class AppendObservation(probeRef: ProbeRef,
                             timestamp: Timestamp,
                             observation: Observation) extends IngestServiceCommand
case class AppendObservationResult(op: AppendObservation) extends IngestServiceResult

case class ListPartitions() extends IngestServiceQuery
case class ListPartitionsResult(op: ListPartitions, partitions: Vector[String]) extends IngestServiceResult

case class GetObservations(partition: String, count: Int, token: Option[String]) extends IngestServiceQuery
case class GetObservationsResult(op: GetObservations, observations: Vector[Observation], token: String) extends IngestServiceResult

case class GetCheckpoint(partition: String) extends IngestServiceQuery
case class GetCheckpointResult(op: GetCheckpoint, token: String) extends IngestServiceResult

case class PutCheckpoint(partition: String, token: String) extends IngestServiceCommand
case class PutCheckpointResult(op: PutCheckpoint) extends IngestServiceResult
