package io.mandelbrot.core.system

import akka.actor.{ActorSystem, Terminated}
import akka.testkit.{TestProbe, ImplicitSender, TestActorRef, TestKit}
import org.joda.time.DateTime
import org.scalatest.ShouldMatchers
import org.scalatest.{WordSpecLike, BeforeAndAfterAll}
import scala.concurrent.duration._

import io.mandelbrot.core.model._
import io.mandelbrot.core.state._
import io.mandelbrot.core.metrics._
import io.mandelbrot.core.{AkkaConfig, Blackhole}
import io.mandelbrot.core.ConfigConversions._

class ProbeSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ProbeSpec", AkkaConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val blackhole = system.actorOf(Blackhole.props())

  "A Probe" should {

    "have an initial state" in {
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.TestBehavior"
      val factory = ProbeBehavior.extensions(probeType).configure(Map.empty)
      val services = system.actorOf(TestServiceProxy.props())
      val metricsBus = new MetricsBus()
      val actor = TestActorRef(new Probe(ProbeRef("fqdn:local/"), blackhole, probeType, Set.empty, policy, factory, 0, services, metricsBus))
      val probe = actor.underlyingActor
      probe.lifecycle shouldEqual ProbeInitializing
      probe.health shouldEqual ProbeUnknown
      probe.summary shouldEqual None
      probe.lastChange shouldEqual None
      probe.lastUpdate shouldEqual None
      probe.correlationId shouldEqual None
      probe.acknowledgementId shouldEqual None
      probe.squelch shouldEqual false
    }

    "initialize and transition to running behavior" in {
      val ref = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.TestBehavior"
      val factory = ProbeBehavior.extensions(probeType).configure(Map.empty)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val actor = system.actorOf(Probe.props(ref, blackhole, probeType, Set.empty, policy, factory, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeKnown, None, ProbeHealthy, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initialize, Some(status)))

      actor ! GetProbeStatus(ref)
      val result = expectMsgClass(classOf[GetProbeStatusResult])
      result.status.lifecycle shouldEqual ProbeKnown
      result.status.health shouldEqual ProbeHealthy
      result.status.summary shouldEqual None
      result.status.correlation shouldEqual None
      result.status.acknowledged shouldEqual None
      result.status.squelched shouldEqual false
    }

    "update behavior" in {
      val ref = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.TestBehavior"
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val factory1 = ProbeBehavior.extensions(probeType).configure(Map("key" -> "value1"))

      val actor = TestActorRef(new Probe(ref, blackhole, probeType, Set.empty, policy, factory1, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeKnown, None, ProbeHealthy, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initialize, Some(status)))

      actor.underlyingActor.processor shouldBe a [TestProcessor]
      actor.underlyingActor.processor should have ('properties (Map("key" -> "value1")))

      val factory2 = ProbeBehavior.extensions(probeType).configure(Map("key" -> "value2"))
      actor ! ChangeProbe(probeType, policy, factory2, Set.empty, 1)
      val update = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(update))

      actor.underlyingActor.processor shouldBe a [TestProcessor]
      actor.underlyingActor.processor should have ('properties (Map("key" -> "value2")))
    }

    "change behaviors" in {
      val ref = ProbeRef("fqdn:local/")
      val child1 = ProbeRef("fqdn:local/child1")
      val child2 = ProbeRef("fqdn:local/child2")
      val child3 = ProbeRef("fqdn:local/child3")
      val children = Set(child1, child2, child3)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()
      val policy = ProbePolicy(1.minute, 2.seconds, 1.minute, 1.minute, None)
      val probeType1 = "io.mandelbrot.core.system.TestBehavior"
      val factory1 = ProbeBehavior.extensions(probeType1).configure(Map.empty)

      val actor = TestActorRef(new Probe(ref, blackhole, probeType1, children, policy, factory1, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initialize, Some(status)))

      val probeType2 = "io.mandelbrot.core.system.TestBehavior"
      val factory2 = ProbeBehavior.extensions(probeType2).configure(Map.empty)
      actor ! ChangeProbe(probeType2, policy, factory2, children, 1)
      val update = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(update))

      actor.underlyingActor.children shouldEqual children
      actor.underlyingActor.policy shouldEqual policy
      actor.underlyingActor.processor shouldBe a [TestProcessorChange]
    }

    "transition to retired behavior" ignore {
      val ref = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.TestBehavior"
      val factory = ProbeBehavior.extensions(probeType).configure(Map.empty)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val actor = system.actorOf(Probe.props(ref, blackhole, probeType, Set.empty, policy, factory, 0, services, metricsBus))
      watch(actor)

      val initialize = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeKnown, None, ProbeHealthy, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initialize, Some(status)))

      actor ! RetireProbe(0)
      val retire = stateService.expectMsgClass(classOf[DeleteProbeStatus])
      stateService.reply(DeleteProbeStatusResult(retire))

      val result = expectMsgClass(classOf[Terminated])
      result.actor shouldEqual actor
    }
  }
}
