package io.mandelbrot.core.check

import java.util.UUID

import akka.actor.{PoisonPill, ActorRef, ActorSystem}
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import org.HdrHistogram.DoubleHistogram
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpecLike}
import scala.concurrent.duration._

import io.mandelbrot.core.{MandelbrotConfig, ServiceProxy, AkkaConfig}
import io.mandelbrot.core.ConfigConversions._
import io.mandelbrot.core.metrics.{PutProbeMetricsResult, PutProbeMetrics}
import io.mandelbrot.core.model._
import io.mandelbrot.core.parser.TimeseriesEvaluationParser

class TimeseriesEvaluationProcessorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("TimeseriesEvaluationProcessorSpec", AkkaConfig ++ MandelbrotConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  def withServiceProxy(testCode: (ActorRef) => Any) {
    val services = system.actorOf(ServiceProxy.props())
    testCode(services)
    services ! PoisonPill
  }

  val probeId = ProbeId("system.load")
  val metricName = "load1"
  val statistic = Metric90thPercentile
  val samplingRate = PerMinute
  val dimension = Dimension("agent", "foo")

  val timestamp1 = Timestamp() - 2.minute
  val id1 = UUID.randomUUID()
  val histogram1 = new DoubleHistogram(3)
  histogram1.recordValue(0.1)

  val timestamp2 = timestamp1 - 1.minute
  val id2 = UUID.randomUUID()
  val histogram2 = new DoubleHistogram(3)
  histogram2.recordValue(0.2)

  val timestamp3 = timestamp1 - 2.minutes
  val id3 = UUID.randomUUID()
  val histogram3 = new DoubleHistogram(3)
  histogram3.recordValue(0.3)

  val timestamp4 = timestamp1 - 3.minutes
  val id4 = UUID.randomUUID()
  val histogram4 = new DoubleHistogram(3)
  histogram4.recordValue(0.4)

  val timestamp5 = timestamp1 - 4.minutes
  val id5 = UUID.randomUUID()
  val histogram5 = new DoubleHistogram(3)
  histogram5.recordValue(0.5)

  def withTestData(testCode: (ActorRef) => Any): Unit = {
    withServiceProxy { services =>

      services ! PutProbeMetrics(probeId, timestamp1, metricName, dimension, histogram1, id1)
      expectMsgClass(classOf[PutProbeMetricsResult])

      services ! PutProbeMetrics(probeId, timestamp2, metricName, dimension, histogram2, id2)
      expectMsgClass(classOf[PutProbeMetricsResult])

      services ! PutProbeMetrics(probeId, timestamp3, metricName, dimension, histogram3, id3)
      expectMsgClass(classOf[PutProbeMetricsResult])

      services ! PutProbeMetrics(probeId, timestamp4, metricName, dimension, histogram4, id4)
      expectMsgClass(classOf[PutProbeMetricsResult])

      services ! PutProbeMetrics(probeId, timestamp5, metricName, dimension, histogram5, id5)
      expectMsgClass(classOf[PutProbeMetricsResult])

      testCode(services)
    }
  }

  "A TimeseriesEvaluationProcessor" should {

    "process a timeseries evaluation" in withTestData { services =>

      // configure processor
      val generation = 1L
      val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation(
        """
          |MIN(probe:system.load:load1:p90:1minute:agent=foo) > 1 OVER 5 SAMPLES
        """.stripMargin)
      val metricsService = new TestProbe(_system)
      val settings = TimeseriesEvaluationSettings(evaluation)
      val processor = system.actorOf(TimeseriesEvaluationProcessor.props(settings, timeDilation = 20))
      processor ! ChangeProcessor(lsn = 1, services, Set.empty)

      val processorStatus = expectMsgClass(10.seconds, classOf[ProcessorStatus])
      processorStatus.lsn shouldEqual 1
      processorStatus.health shouldEqual CheckHealthy
    }
  }
}

