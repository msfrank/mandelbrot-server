package io.mandelbrot.core.check

import akka.actor.ActorSystem
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import io.mandelbrot.core.parser.TimeseriesEvaluationParser
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpecLike}

import io.mandelbrot.core.{TestServiceProxy, AkkaConfig}
import io.mandelbrot.core.ConfigConversions._

class TimeseriesEvaluationProcessorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("TimeseriesEvaluationProcessorSpec", AkkaConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  "A TimeseriesEvaluationProcessor" should {

    "process a timeseries evaluation" in {

      // configure processor
      val generation = 1L
      val evaluation = TimeseriesEvaluationParser.parseTimeseriesEvaluation(
        """
          |MIN(probe:system.load:p90:1minute:agent=foo) > 0 OVER 5 SAMPLES
        """.stripMargin)
      val metricsService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(metricsService = Some(metricsService.ref)))
      val settings = TimeseriesEvaluationSettings(evaluation)
      val processor = system.actorOf(TimeseriesEvaluationProcessor.props(settings))

      //
    }
  }
}

