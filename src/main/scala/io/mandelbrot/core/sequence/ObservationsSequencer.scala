/**
  * Copyright 2015 Michael Frank <msfrank@syntaxjockey.com>
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

package io.mandelbrot.core.sequence

import java.io.File
import java.util.UUID

import akka.actor._
import io.mandelbrot.core.model.{Timestamp, PerMinute, Observation}
import scala.concurrent.duration._

import io.mandelbrot.core.ingest._
import io.mandelbrot.core.timeseries.Tick

/**
 *
 */
class ObservationsSequencer(val services: ActorRef, partitionId: String, partitionDir: File, limit: Int)
  extends LoggingFSM[ObservationsSequencer.State,ObservationsSequencer.Data] {
  import ObservationsSequencer._

  // config
  val segmentInterval = 1.minute
  val persistThresholdByTime = 10.seconds
  val persistThresholdByRecords = 1000
  val getObservationRetryInterval = 5.seconds
  val segmentWindow = 5.minutes

  // state
  var segments: Map[Tick,ActorRef] = Map.empty
  var numConsumed: Int = 0
  var numPersisted: Int = 0

  override def preStart(): Unit = {
    super.preStart()
    setTimer("flush-segments", FlushSegments, segmentInterval, repeat = true)
    services ! GetCheckpoint(partitionId)
  }

  /*
   *
   */
  when(Initializing) {

    /* */
    case Event(GetCheckpointResult(op, token), data: Initializing) =>
      goto(Consuming) using Consuming(token, Vector.empty)

    /* */
    case Event(IngestServiceOperationFailed(op: GetCheckpoint, failure), data: Initializing) =>
      throw failure
  }

  onTransition {
    case _ -> Consuming =>
      val current = Timestamp() - segmentWindow
      val expiredSegments = segments.filterKeys(_ < current)
      expiredSegments.foreach { case (tick, ref) =>
        context.stop(ref)
        segments = segments - tick
      }
      numConsumed = 0
      numPersisted = 0
      setTimer("persist-segments", PersistSegments, persistThresholdByTime, repeat = false)
      self ! ReadFromPartition
  }

  /*
   *
   */
  when(Consuming) {

    /* */
    case Event(ReadFromPartition, data: Consuming) =>
      services ! GetObservations(partitionId, limit, Some(data.token))
      stay()

    /* */
    case Event(result: GetObservationsResult, data: Consuming) if result.observations.isEmpty =>
      setTimer("read-from-partition", ReadFromPartition, getObservationRetryInterval, repeat = false)
      stay()

    /* */
    case Event(GetObservationsResult(op, observations, token), data: Consuming) =>
      numConsumed = numConsumed + observations.length
      val inflight = data.inflight ++ observations
      if (numConsumed > persistThresholdByRecords) {
        val observationsByTick = inflight
          .groupBy(observation => Tick(observation.timestamp.getMillis, PerMinute))
        goto(Persisting) using Persisting(token, observationsByTick)
      } else stay() using Consuming(token, inflight)

    /* */
    case Event(IngestServiceOperationFailed(op, failure), data: Consuming) =>
      throw failure

    case Event(PersistSegments, data: Consuming) =>
      val observationsByTick = data.inflight
        .groupBy(observation => Tick(observation.timestamp.getMillis, PerMinute))
      goto(Persisting) using Persisting(data.token, observationsByTick)

    case Event(FlushSegments, data: Consuming) =>
      stay()
  }

  onTransition {
    case Consuming -> Persisting =>
      cancelTimer("persist-segments")
      cancelTimer("read-from-partition")
      self ! PersistSegments
  }

  /*
   *
   */
  when(Persisting) {

    case Event(PersistSegments, data: Persisting) =>
      data.inflight.foreach { case (tick,batch) =>
        val writer = segments.getOrElse(tick, context.actorOf(makeWriterProps(tick)))
        segments = segments + (tick -> writer)
        val op = PersistObservations(batch)
        writer ! op
      }
      stay()

    case Event(PersistObservationsResult(op, tick), data: Persisting) =>
      val inflight = data.inflight.get(tick) match {
        case Some(batch) =>
          numPersisted = numPersisted + batch.length
          data.inflight - tick
        case None => data.inflight
      }
      if (inflight.nonEmpty) {
        stay() using Persisting(data.token, inflight)
      } else {
        goto(Updating) using Updating(data.token)
      }

    case Event(ObservationsWriterOperationFailed(op, failure), data: Persisting) =>
      throw failure
  }

  onTransition {
    case Consuming -> Updating =>
  }

  /*
   *
   */
  when(Updating) {
    case Event(_, _) =>
      stay()
  }

  def makeWriterProps(tick: Tick): Props = {
    val uuid = UUID.randomUUID()
    val file = new File(partitionDir, s"${tick.instant}_${tick.interval}_${uuid.toString}.segment")
    ObservationsWriter.props(tick, uuid, file)
  }

  override def postStop(): Unit = {
    cancelTimer("flush-segments")
    super.postStop()
  }

  initialize()
}

object ObservationsSequencer {
  def props(services: ActorRef, partitionId: String, partitionDir: File, limit: Int) = {
    Props(classOf[ObservationsSequencer], services, partitionId, partitionDir, limit)
  }

  sealed trait State
  case object Initializing extends State
  case object Consuming extends State
  case object Persisting extends State
  case object Updating extends State

  sealed trait Data
  case class Initializing() extends Data
  case class Consuming(token: String, inflight: Vector[Observation]) extends Data
  case class Persisting(token: String, inflight: Map[Tick,Vector[Observation]]) extends Data
  case class Updating(token: String) extends Data

  case object FlushSegments
  case object ReadFromPartition
  case object PersistSegments
}
