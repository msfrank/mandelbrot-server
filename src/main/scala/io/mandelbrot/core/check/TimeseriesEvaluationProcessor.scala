package io.mandelbrot.core.check

import akka.actor._
import io.mandelbrot.core.parser.TimeseriesEvaluationParser
import spray.json.DefaultJsonProtocol._
import spray.json._

import io.mandelbrot.core.model.{Timestamp, MetricSource}
import io.mandelbrot.core.metrics._
import io.mandelbrot.core.timeseries._

case class TimeseriesEvaluationSettings(evaluation: TimeseriesEvaluation)

class TimeseriesCheck extends ProcessorExtension {
  type Settings = TimeseriesEvaluationSettings
  implicit object TimeseriesEvalutionFormat extends JsonFormat[TimeseriesEvaluation] {
    override def write(obj: TimeseriesEvaluation): JsValue = JsString(obj.toString)
    override def read(json: JsValue): TimeseriesEvaluation = json match {
      case JsString(string) => TimeseriesEvaluationParser.parseTimeseriesEvaluation(string)
      case other => throw new DeserializationException("")
    }
  }
  implicit val TimeseriesEvaluationSettingsFormat = jsonFormat1(TimeseriesEvaluationSettings)
  def configure(json: Option[JsObject]) = json.map(_.convertTo[TimeseriesEvaluationSettings])
    .getOrElse(throw new Exception(""))
  def props(settings: TimeseriesEvaluationSettings) = TimeseriesEvaluationProcessor.props(settings)
}

/**
 *
 */
class TimeseriesEvaluationProcessor(settings: TimeseriesEvaluationSettings) extends Actor with ActorLogging {
  import context.dispatcher
  import TimeseriesEvaluationProcessor.AdvanceTick

  // config
  val evaluation = settings.evaluation

  // state
  var lsn: Long = 0
  var services: ActorRef = ActorRef.noSender
  var timestamp = Timestamp()
  var currentTick: Tick = Tick(timestamp, evaluation.samplingRate) - 1
  val timeseriesStore = new TimeseriesStore(evaluation, initialInstant = Some(currentTick.toTimestamp))
  var advanceTick: Option[Cancellable] = None
  var inflight: Set[MetricsServiceOperation] = Set.empty

  /**
   *
   */
  def incubating: Receive = {

    case change: ChangeProcessor =>
      lsn = change.lsn
      services = change.services
      context.become(running)

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

  /**
   *
   */
  def running: Receive = {

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
      context.parent ! LsnTickAlarmState(lsn, currentTick, evaluation.evaluate(timeseriesStore))
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

  def receive = incubating
}

object TimeseriesEvaluationProcessor {
  def props(settings: TimeseriesEvaluationSettings) = Props(classOf[TimeseriesEvaluationProcessor], settings)
  case object AdvanceTick
}
