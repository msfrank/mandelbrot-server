package io.mandelbrot.core.metrics

import akka.actor.{Props, ActorLogging, Actor}
import com.typesafe.config.Config
import io.mandelbrot.core.{ApiException, ResourceNotFound}
import org.HdrHistogram.DoubleHistogram
import java.util.UUID
import org.joda.time.DateTime

import scala.collection.JavaConversions._

import io.mandelbrot.core.model._

class TestMetricsImplementation(settings: TestMetricsExtensionSettings) extends Actor with ActorLogging {

  val metrics = new java.util.HashMap[ProbeIdMetricNameDimension,java.util.TreeMap[Timestamp,Map[UUID,DoubleHistogram]]]

  def receive = {

    /* */
    case op: PutProbeMetrics =>
      val probeIdDimension = ProbeIdMetricNameDimension(op.probeId, op.metricName, op.dimension)
      val histogram = op.value.copy()
      metrics.get(probeIdDimension) match {
        case null =>
          val history = new java.util.TreeMap[Timestamp,Map[UUID,DoubleHistogram]]()
          history.put(op.timestamp, Map(op.id -> histogram))
          sender() ! PutProbeMetricsResult(op)
        case history =>
          history.get(op.timestamp) match {
            case null =>
              history.put(op.timestamp, Map(op.id -> histogram))
            case values =>
              values.put(op.id, histogram)
          }
          sender() ! PutProbeMetricsResult(op)
      }

    /* */
    case op: GetProbeMetricsHistory =>
      try {
        val probeIdDimension = ProbeIdMetricNameDimension(op.probeId, op.metricName, op.dimension)
        val page: ProbeMetricsPage = metrics.get(probeIdDimension) match {
          // if there is no metric, then raise ResourceNotFound
          case null =>
            throw ApiException(ResourceNotFound)
          // if no timeseries params are specified, return the last entry
          case history if op.from.isEmpty && op.to.isEmpty =>
            val values: Vector[ProbeMetrics] = history.lastEntry() match {
              case null => Vector.empty
              case entry =>
                val histograms = entry.getValue.values.toVector
                val histogram = HistogramOps.mergeHistograms(histograms.head, histograms.tail)
                val statistics = HistogramOps.histogram2statisticValues(histogram, op.statistics)
                Vector(ProbeMetrics(op.probeId, op.metricName, op.dimension, entry.getKey, statistics))
            }
            ProbeMetricsPage(values, None, exhausted = true)
          // otherwise return the subset of entries
          case history =>
            val from: Timestamp = op.last.map(string2timestamp)
              .getOrElse(op.from.map(Timestamp.apply).getOrElse(Timestamp.SMALLEST_TIMESTAMP))
            val fromInclusive: Boolean = if (op.last.isDefined) false else op.fromInclusive
            val to: Timestamp = op.to.map(Timestamp.apply).getOrElse(Timestamp.LARGEST_TIMESTAMP)
            val values: Vector[ProbeMetrics] = history.subMap(from, fromInclusive, to, !op.toExclusive).map {
              case entry =>
                val histograms = entry._2.values.toVector
                val histogram = HistogramOps.mergeHistograms(histograms.head, histograms.tail)
                val statistics = HistogramOps.histogram2statisticValues(histogram, op.statistics)
                ProbeMetrics(op.probeId,op.metricName, op.dimension, entry._1, statistics)
            }.toVector
            val trimmed = values.take(op.limit)
            val last = if (values.length > trimmed.length) {
              Some(timestamp2string(trimmed.last.timestamp))
            } else None
            ProbeMetricsPage(trimmed, last, exhausted = last.isEmpty)
        }
        sender() ! GetProbeMetricsHistoryResult(op, page)
      } catch {
        case ex: Throwable => sender() ! MetricsServiceOperationFailed(op, ex)
      }
  }

  def string2timestamp(string: String): Timestamp = Timestamp(string.toLong)

  def timestamp2string(timestamp: Timestamp): String = timestamp.toMillis.toHexString
}

object TestMetricsImplementation {
  def props(settings: TestMetricsExtensionSettings) = Props(classOf[TestMetricsImplementation], settings)
}

case class ProbeIdMetricNameDimension(probeId: ProbeId, metricName: String, dimension: Dimension)

case class TestMetricsExtensionSettings()

class TestMetricsExtension extends MetricsExtension {
  type Settings = TestMetricsExtensionSettings
  def configure(config: Config): Settings = TestMetricsExtensionSettings()
  def props(settings: Settings): Props = TestMetricsImplementation.props(settings)
}
