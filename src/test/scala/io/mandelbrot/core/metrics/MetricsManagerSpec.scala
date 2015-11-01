package io.mandelbrot.core.metrics

import java.util.UUID

import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpecLike}
import akka.actor.{PoisonPill, ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import org.HdrHistogram.DoubleHistogram
import scala.concurrent.duration._

import io.mandelbrot.core._
import io.mandelbrot.core.model._
import io.mandelbrot.core.ConfigConversions._

class MetricsManagerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("MetricsManagerSpec", AkkaConfig ++ MandelbrotConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val settings = ServerConfig(system).settings

  def withMetricsService(testCode: (ActorRef) => Any) {
    val metricsService = system.actorOf(MetricsManager.props(settings.metrics, settings.cluster.enabled))
    testCode(metricsService)
    metricsService ! PoisonPill
  }

  val probeId = ProbeId("system.load")
  val dimension = Dimension("agent", "foo.agent")
  val metricName = "load1"

  val timestamp1 = Timestamp()
  val id1 = UUID.randomUUID()
  val histogram1 = new DoubleHistogram(3)
  histogram1.recordValue(0.1)
  val metrics1 = ProbeMetrics(probeId, metricName, dimension, timestamp1, Map(MetricSampleCount -> 1.0))

  val timestamp2 = timestamp1 + 5.seconds
  val id2 = UUID.randomUUID()
  val histogram2 = new DoubleHistogram(3)
  histogram2.recordValue(0.1)
  histogram2.recordValue(0.2)
  val metrics2 = ProbeMetrics(probeId, metricName, dimension, timestamp2, Map(MetricSampleCount -> 2.0))

  val timestamp3 = timestamp2 + 5.seconds
  val id3 = UUID.randomUUID()
  val histogram3 = new DoubleHistogram(3)
  histogram3.recordValue(0.1)
  histogram3.recordValue(0.2)
  histogram3.recordValue(0.3)
  val metrics3 = ProbeMetrics(probeId, metricName, dimension, timestamp3, Map(MetricSampleCount -> 3.0))

  val timestamp4 = timestamp3 + 5.seconds
  val id4 = UUID.randomUUID()
  val histogram4 = new DoubleHistogram(3)
  histogram4.recordValue(0.1)
  histogram4.recordValue(0.2)
  histogram4.recordValue(0.3)
  histogram4.recordValue(0.4)
  val metrics4 = ProbeMetrics(probeId, metricName, dimension, timestamp4, Map(MetricSampleCount -> 4.0))

  val timestamp5 = timestamp4 + 5.seconds
  val id5 = UUID.randomUUID()
  val histogram5 = new DoubleHistogram(3)
  histogram5.recordValue(0.1)
  histogram5.recordValue(0.2)
  histogram5.recordValue(0.3)
  histogram5.recordValue(0.4)
  histogram5.recordValue(0.5)
  val metrics5 = ProbeMetrics(probeId, metricName, dimension, timestamp5, Map(MetricSampleCount -> 5.0))

  def withTestData(testCode: (ActorRef) => Any): Unit = {
    withMetricsService { metricsService =>

      metricsService ! PutProbeMetrics(probeId, timestamp1, metricName, dimension, histogram1, id1)
      expectMsgClass(classOf[PutProbeMetricsResult])

      metricsService ! PutProbeMetrics(probeId, timestamp2, metricName, dimension, histogram2, id2)
      expectMsgClass(classOf[PutProbeMetricsResult])

      metricsService ! PutProbeMetrics(probeId, timestamp3, metricName, dimension, histogram3, id3)
      expectMsgClass(classOf[PutProbeMetricsResult])

      metricsService ! PutProbeMetrics(probeId, timestamp4, metricName, dimension, histogram4, id4)
      expectMsgClass(classOf[PutProbeMetricsResult])

      metricsService ! PutProbeMetrics(probeId, timestamp5, metricName, dimension, histogram5, id5)
      expectMsgClass(classOf[PutProbeMetricsResult])

      testCode(metricsService)
    }
  }

  "A MetricsManager" when {

    "servicing a PutProbeMetrics request" should {

      "insert metrics for the specified probe" in withMetricsService { metricsService =>
        val timestamp = Timestamp()
        val histogram = new DoubleHistogram(3)
        histogram.recordValue(0.1)
        histogram.recordValue(0.2)
        histogram.recordValue(0.5)
        histogram.recordValue(1.0)
        histogram.recordValue(10.0)
        val id = UUID.randomUUID()
        metricsService ! PutProbeMetrics(probeId, timestamp, metricName, dimension, histogram, id)
        val putProbeMetricsResult = expectMsgClass(classOf[PutProbeMetricsResult])
      }
    }

    "servicing a GetProbeMetricsHistory request" should {

      "return ResourceNotFound if the metric doesn't exist" in withMetricsService { metricsService =>
        metricsService ! GetProbeMetricsHistory(probeId, "doesntexist", dimension, Set(MetricSampleCount), None, None, 10)
        val getProbeMetricsHistoryResult = expectMsgClass(classOf[MetricsServiceOperationFailed])
        getProbeMetricsHistoryResult.failure shouldEqual ApiException(ResourceNotFound)
      }

      "return metrics from the beginning in ascending order if timeseries parameters are not specified" in withTestData { metricsService =>
        metricsService ! GetProbeMetricsHistory(probeId, metricName, dimension, Set(MetricSampleCount), None, None, 10)
        val getProbeMetricsHistoryResult = expectMsgClass(classOf[GetProbeMetricsHistoryResult])
        getProbeMetricsHistoryResult.page.history shouldEqual Vector(metrics1, metrics2, metrics3, metrics4, metrics5)
        getProbeMetricsHistoryResult.page.last shouldEqual None
        getProbeMetricsHistoryResult.page.exhausted shouldEqual true
      }

      "return metrics from the end in descending order if timeseries parameters are not specified and descending is true" in withTestData { metricsService =>
        metricsService ! GetProbeMetricsHistory(probeId, metricName, dimension, Set(MetricSampleCount), None, None, 10, descending = true)
        val getProbeMetricsHistoryResult = expectMsgClass(classOf[GetProbeMetricsHistoryResult])
        getProbeMetricsHistoryResult.page.history shouldEqual Vector(metrics5, metrics4, metrics3, metrics2, metrics1)
        getProbeMetricsHistoryResult.page.last shouldEqual None
        getProbeMetricsHistoryResult.page.exhausted shouldEqual true
      }

      "return a page of metric history newer than 'from' when 'from' is specified" in withTestData { metricsService =>
        metricsService ! GetProbeMetricsHistory(probeId, metricName, dimension, Set(MetricSampleCount), Some(timestamp3.toDateTime), None, 100)
        val getProbeMetricsHistoryResult = expectMsgClass(classOf[GetProbeMetricsHistoryResult])
        getProbeMetricsHistoryResult.page.history shouldEqual Vector(metrics4, metrics5)
        getProbeMetricsHistoryResult.page.last shouldEqual None
        getProbeMetricsHistoryResult.page.exhausted shouldEqual true
      }

      "return a page of metric history older or equal to 'to' when 'to' is specified" in withTestData { metricsService =>
        metricsService ! GetProbeMetricsHistory(probeId, metricName, dimension, Set(MetricSampleCount), None, Some(timestamp4.toDateTime), 100)
        val getProbeMetricsHistoryResult = expectMsgClass(classOf[GetProbeMetricsHistoryResult])
        getProbeMetricsHistoryResult.page.history shouldEqual Vector(metrics1, metrics2, metrics3, metrics4)
        getProbeMetricsHistoryResult.page.last shouldEqual None
        getProbeMetricsHistoryResult.page.exhausted shouldEqual true
      }

      "return a page of metric history strictly older than 'to' when 'to' is specified and toExclusive is true" in withTestData { metricsService =>
        metricsService ! GetProbeMetricsHistory(probeId, metricName, dimension, Set(MetricSampleCount), None, Some(timestamp4.toDateTime), 100, toExclusive = true)
        val getProbeMetricsHistoryResult = expectMsgClass(classOf[GetProbeMetricsHistoryResult])
        getProbeMetricsHistoryResult.page.history shouldEqual Vector(metrics1, metrics2, metrics3)
        getProbeMetricsHistoryResult.page.last shouldEqual None
        getProbeMetricsHistoryResult.page.exhausted shouldEqual true
      }

      "return a page of metric history between 'from' and 'to' when 'from' and 'to' are specified" in withTestData { metricsService =>
        metricsService ! GetProbeMetricsHistory(probeId, metricName, dimension, Set(MetricSampleCount), Some(timestamp2.toDateTime), Some(timestamp4.toDateTime), 100)
        val getProbeMetricsHistoryResult = expectMsgClass(classOf[GetProbeMetricsHistoryResult])
        getProbeMetricsHistoryResult.page.history shouldEqual Vector(metrics3, metrics4)
        getProbeMetricsHistoryResult.page.last shouldEqual None
        getProbeMetricsHistoryResult.page.exhausted shouldEqual true
      }
    }
  }

}