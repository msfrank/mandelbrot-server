package io.mandelbrot.core.check

import akka.actor._

import io.mandelbrot.core.timeseries.{Tick, TimeseriesStore, TimeseriesEvaluation}
import io.mandelbrot.core.metrics.{MetricsServiceOperation, MetricsServiceOperationFailed, GetProbeMetricsHistoryResult, GetProbeMetricsHistory}
import io.mandelbrot.core.model.{Timestamp, MetricSource}

/**
 *
 */
class TimeseriesEvaluationProcessor(generation: Long,
                                    evaluation: TimeseriesEvaluation,
                                    services: ActorRef) extends Actor with ActorLogging {
  import context.dispatcher
  import TimeseriesEvaluationProcessor.AdvanceTick

  // state
  var timestamp = Timestamp()
  var currentTick: Tick = Tick(timestamp, evaluation.samplingRate) - 1
  val timeseriesStore = new TimeseriesStore(evaluation, initialInstant = Some(currentTick.toTimestamp))
  var advanceTick: Option[Cancellable] = None
  var inflight: Set[MetricsServiceOperation] = Set.empty

  override def preStart(): Unit = {

    val duration = currentTick.toDuration
    advanceTick = Some(context.system.scheduler.schedule(duration, duration, self, AdvanceTick))

    inflight = timeseriesStore.windows().map {
      case (source @ MetricSource(probeId, metricName, statistic, samplingRate, dimension), window) =>
        val from = Some(window.horizon.toTimestamp.toDateTime)
        val to = Some(window.tip.toTimestamp.toDateTime)
        val op = GetProbeMetricsHistory(probeId, metricName, dimension, Set(statistic),
          samplingRate, from, to, limit = window.size, fromInclusive = true)
        services ! op
        op
    }.toSet
  }

  def receive = {

    /* */
    case result: GetProbeMetricsHistoryResult if !inflight.contains(result.op) =>
      // do nothing

    /* */
    case result: GetProbeMetricsHistoryResult =>
      inflight = inflight - result.op
      val source = MetricSource(result.op.probeId, result.op.metricName,
        result.op.statistics.head, result.op.samplingRate, result.op.dimension)
      result.page.history.foreach { metrics => timeseriesStore.put(source, metrics) }
      if (!result.page.exhausted) {
        val op = result.op.copy(last = result.page.last)
        services ! op
        inflight = inflight + op
      }

    /* */
    case AdvanceTick =>
      context.parent ! GenerationTickAlarmState(generation, currentTick, evaluation.evaluate(timeseriesStore))
      timestamp = Timestamp()
      currentTick = currentTick + 1
      timeseriesStore.advance(currentTick.toTimestamp)
      inflight = timeseriesStore.windows().map {
        case (source @ MetricSource(probeId, metricName, statistic, samplingRate, dimension), window) =>
          val from = Some(window.horizon.toTimestamp.toDateTime)
          val to = Some(window.tip.toTimestamp.toDateTime)
          val op = GetProbeMetricsHistory(probeId, metricName, dimension, Set(statistic),
            samplingRate, from, to, limit = window.size, fromInclusive = true)
          services ! op
          op
      }.toSet

    /* for now, just drop failures on the floor */
    case failure: MetricsServiceOperationFailed =>
      log.debug("failed to get probe metrics history: {}", failure.failure)
      inflight = inflight - failure.op
  }
}

object TimeseriesEvaluationProcessor {
  def props(generation: Long, evaluation: TimeseriesEvaluation, services: ActorRef) = {
    Props(classOf[TimeseriesEvaluationProcessor], generation, evaluation, services)
  }
  case object AdvanceTick
}

case class GenerationTickAlarmState(generation: Long, tick: Tick, alarmState: Option[Boolean])
