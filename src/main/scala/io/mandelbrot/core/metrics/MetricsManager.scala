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

package io.mandelbrot.core.metrics

import java.util.UUID

import akka.actor._
import org.HdrHistogram.DoubleHistogram

import io.mandelbrot.core._
import io.mandelbrot.core.model._
import org.joda.time.DateTime

/**
 *
 */
class MetricsManager(settings: MetricsSettings, clusterEnabled: Boolean) extends Actor with ActorLogging {

  // config
  val impl: ActorRef = context.actorOf(settings.props, "metrics-impl")

  def receive = {

    case op: PutProbeMetrics =>
      impl forward op

    case op: GetProbeMetricsHistory =>
      impl forward op

    case unhandled =>
      log.error("dropping unhandled message {}", unhandled)
  }
}

object MetricsManager {
  def props(settings: MetricsSettings, clusterEnabled: Boolean) = {
    Props(classOf[MetricsManager], settings, clusterEnabled)
  }
}

/**
 *
 */
sealed trait MetricsServiceOperation extends ServiceOperation
sealed trait MetricsServiceCommand extends ServiceCommand with MetricsServiceOperation
sealed trait MetricsServiceQuery extends ServiceQuery with MetricsServiceOperation
sealed trait MetricsServiceResult
case class MetricsServiceOperationFailed(op: MetricsServiceOperation, failure: Throwable) extends ServiceOperationFailed

case class PutProbeMetrics(probeId: ProbeId,
                           timestamp: Timestamp,
                           metricName: String,
                           dimension: Dimension,
                           value: DoubleHistogram,
                           id: UUID) extends MetricsServiceCommand
case class PutProbeMetricsResult(op: PutProbeMetrics) extends MetricsServiceResult

case class GetProbeMetricsHistory(probeId: ProbeId,
                                  metricName: String,
                                  dimension: Dimension,
                                  statistics: Set[Statistic],
                                  from: Option[DateTime],
                                  to: Option[DateTime],
                                  limit: Int,
                                  fromInclusive: Boolean = false,
                                  toExclusive: Boolean = false,
                                  descending: Boolean = false,
                                  last: Option[String] = None) extends MetricsServiceQuery
case class GetProbeMetricsHistoryResult(op: GetProbeMetricsHistory, page: ProbeMetricsPage) extends MetricsServiceResult
