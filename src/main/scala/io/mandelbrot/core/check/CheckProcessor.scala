package io.mandelbrot.core.check

import java.util.concurrent.TimeUnit

import akka.actor._
import scala.concurrent.duration._

import io.mandelbrot.core.timeseries.{Tick, TimeseriesStore, TimeseriesEvaluation}
import io.mandelbrot.core.metrics.{MetricsServiceOperationFailed, GetProbeMetricsHistoryResult, GetProbeMetricsHistory}
import io.mandelbrot.core.model.MetricSource

/**
 *
 */
class CheckProcessor(generation: Long,
                     evaluation: TimeseriesEvaluation,
                     services: ActorRef) extends Actor with ActorLogging {
  import context.dispatcher
  import CheckProcessor.ProcessorTick

  // config

  // state
  val timeseriesStore = new TimeseriesStore(evaluation)
  var scheduledTick: Option[Cancellable] = None

  override def preStart(): Unit = {

    scheduledTick = timeseriesStore.samplingRate.map { samplingRate =>
        val duration = FiniteDuration(samplingRate.millis, TimeUnit.MILLISECONDS)
        context.system.scheduler.schedule(duration, duration, self, ProcessorTick)
    }

    timeseriesStore.windows().foreach {
      case (MetricSource(probeId, metricName, dimension, statistic, samplingRate), window) =>
        val from = Some(window.horizon.toTimestamp.toDateTime)
        val to = Some(window.tip.toTimestamp.toDateTime)
        services ! GetProbeMetricsHistory(probeId, metricName, dimension, Set(statistic),
          samplingRate, from, to, limit = window.size, fromInclusive = true)
    }
  }

  def receive = {

    /* */
    case result: GetProbeMetricsHistoryResult =>
      val source = MetricSource(result.op.probeId, result.op.metricName, result.op.dimension,
        result.op.statistics.head, result.op.samplingRate)
      result.page.history.foreach { metrics => timeseriesStore.put(source, metrics) }
      if (!result.page.exhausted) {
        services ! result.op.copy(last = result.page.last)
      }

    /* */
    case ProcessorTick =>

    /* */
    case failure: MetricsServiceOperationFailed =>
  }
}

object CheckProcessor {
  def props(generation: Long, evaluation: TimeseriesEvaluation, services: ActorRef) = {
    Props(classOf[CheckProcessor], generation, evaluation, services)
  }
  case object ProcessorTick
}

case class GenerationTickAlarmState(generation: Long, tick: Tick, alarmState: Option[Boolean])
