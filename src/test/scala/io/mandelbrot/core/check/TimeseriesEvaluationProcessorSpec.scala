package io.mandelbrot.core.check

import akka.actor.ActorSystem
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpecLike}
import scala.concurrent.duration._

import io.mandelbrot.core.{TestServiceProxy, AkkaConfig}
import io.mandelbrot.core.ConfigConversions._
import io.mandelbrot.core.metrics.{GetProbeMetricsHistoryResult, GetProbeMetricsHistory}
import io.mandelbrot.core.model._
import io.mandelbrot.core.parser.TimeseriesEvaluationParser

class TimeseriesEvaluationProcessorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("TimeseriesEvaluationProcessorSpec", AkkaConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  "A TimeseriesEvaluationProcessor" should {

    val probeId = ProbeId("system.load")
    val metricName = "load1"
    val statistic = Metric90thPercentile
    val samplingRate = PerMinute
    val dimension = Dimension("agent", "foo")

    val timestamp = Timestamp()
    val metrics1 = ProbeMetrics(probeId, metricName, dimension, timestamp, Map(statistic -> 0.1))
    val metrics2 = ProbeMetrics(probeId, metricName, dimension, timestamp + 1.minute, Map(statistic -> 0.2))
    val metrics3 = ProbeMetrics(probeId, metricName, dimension, timestamp + 2.minute, Map(statistic -> 0.3))
    val metrics4 = ProbeMetrics(probeId, metricName, dimension, timestamp + 3.minute, Map(statistic -> 0.4))
    val metrics5 = ProbeMetrics(probeId, metricName, dimension, timestamp + 4.minute, Map(statistic -> 0.5))

    "process a timeseries evaluation" in {

      // configure processor
      val generation = 1L
      val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation(
        """
          |MIN(probe:system.load:load1:p90:1minute:agent=foo) > 1 OVER 5 SAMPLES
        """.stripMargin)
      val metricsService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(metricsService = Some(metricsService.ref)))
      val settings = TimeseriesEvaluationSettings(evaluation)
      val processor = system.actorOf(TimeseriesEvaluationProcessor.props(settings, timeDilation = 60))
      processor ! ChangeProcessor(lsn = 1, services, Set.empty)

      //
      val getProbeMetricsHistory = metricsService.expectMsgClass(classOf[GetProbeMetricsHistory])
      getProbeMetricsHistory.probeId shouldEqual probeId
      getProbeMetricsHistory.metricName shouldEqual metricName
      getProbeMetricsHistory.statistics shouldEqual Set(statistic)
      getProbeMetricsHistory.samplingRate shouldEqual samplingRate
      getProbeMetricsHistory.dimension shouldEqual dimension

      val history = Vector(metrics1, metrics2, metrics3, metrics4, metrics5)
      val page = ProbeMetricsPage(history, last = None, exhausted = true)
      val getProbeMetricsHistoryResult = GetProbeMetricsHistoryResult(getProbeMetricsHistory, page)
      metricsService.reply(getProbeMetricsHistoryResult)

      val processorStatus = expectMsgClass(classOf[ProcessorStatus])
      processorStatus.lsn shouldEqual 1
      processorStatus.health shouldEqual CheckHealthy
    }
  }
}

