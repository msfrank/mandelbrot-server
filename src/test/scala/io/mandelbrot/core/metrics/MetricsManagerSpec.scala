package io.mandelbrot.core.metrics

import java.util.UUID

import akka.actor.{PoisonPill, ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import io.mandelbrot.core.model.{Timestamp, Dimension, ProbeId}
import org.HdrHistogram.DoubleHistogram
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpecLike}

import io.mandelbrot.core.{ServerConfig, MandelbrotConfig, AkkaConfig}
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

    }
  }

}