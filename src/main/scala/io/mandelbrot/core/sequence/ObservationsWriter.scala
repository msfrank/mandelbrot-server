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

import java.util.UUID

import akka.actor.{Props, ActorLogging, Actor}
import com.google.common.cache._
import io.mandelbrot.core.timeseries.Tick
import org.HdrHistogram.DoubleHistogram
import java.io.File
import java.nio.charset.Charset

import io.mandelbrot.core.model._

class ObservationsWriter(tick: Tick, id: UUID, dataSegmentFile: File) extends Actor with ActorLogging {

  // config
  val numberOfSignificantValueDigits: Int = 3
  val highestToLowestValueRatio: Long = Long.MaxValue
  val cacheSize = 100

  // state
  val dataSegment = DataSegment.open(dataSegmentFile)
  val cache: LoadingCache[Array[Byte],DoubleHistogram] = CacheBuilder.newBuilder()
    .maximumSize(cacheSize)
    .removalListener { new RemovalListener[Array[Byte],DoubleHistogram] {
      def onRemoval(notification: RemovalNotification[Array[Byte], DoubleHistogram]): Unit = {
        dataSegment.merge(notification.getKey, notification.getValue)
      }
    }}
    .build { new CacheLoader[Array[Byte],DoubleHistogram] {
      def load(key: Array[Byte]): DoubleHistogram = {
        new DoubleHistogram(highestToLowestValueRatio, numberOfSignificantValueDigits)
      }
    }}

  def receive = {

    case op: PersistObservations =>
      op.observations.foreach { observation =>
        observation.dimensions.foreach { case (dimensionName,dimensionValue) =>
          observation match {
            case scalarMap: ScalarMapObservation =>
              scalarMap.scalarMap.foreach { case (metricName, datapoint) =>
                val histogram = cache.get(makeKey(observation.probeId, dimensionName, dimensionValue, metricName))
                histogram.recordValue(datapoint.value)
              }
            case vector: VectorObservation =>
              val histogram = cache.get(makeKey(observation.probeId, dimensionName, dimensionValue, vector.metricName))
              vector.vector.foreach(datapoint => histogram.recordValue(datapoint.value))
          }
        }
      }
      sender() ! PersistObservationsResult(op, tick)


  }

  private def makeKey(probeId: ProbeId, dimensionName: String, dimensionValue: String, metricName: String): Array[Byte] = {
    s"$probeId:$dimensionName:$dimensionValue:$metricName".getBytes(ObservationsWriter.utf8charset)
  }
}

object ObservationsWriter {
  def props(tick: Tick, id: UUID, dataSegmentFile: File) = {
    Props(classOf[ObservationsWriter], tick, id, dataSegmentFile)
  }
  val utf8charset = Charset.forName("UTF-8")
}

sealed trait ObservationsWriterOperation
sealed trait ObservationsWriterCommand extends ObservationsWriterOperation
sealed trait ObservationsWriterQuery extends ObservationsWriterOperation
sealed trait ObservationsWriterResult
case class ObservationsWriterOperationFailed(op: ObservationsWriterOperation, failure: Throwable)

case class PersistObservations(observations: Vector[Observation]) extends ObservationsWriterCommand
case class PersistObservationsResult(op: PersistObservations, tick: Tick) extends ObservationsWriterResult
