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

  def this() = this(ActorSystem("CheckSpec", AkkaConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val blackhole = system.actorOf(Blackhole.props())

  "A Probe" should {

    "have an initial state" in {
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.TestBehavior"
      val factory = ProbeBehavior.extensions(probeType).configure(Map.empty)
      val services = system.actorOf(TestServiceProxy.props())
      val metricsBus = new MetricsBus()
      val probe = TestActorRef(new Probe(ProbeRef("fqdn:local/"), blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory, Set.empty, 0)
      val underlying = probe.underlyingActor
      underlying.lifecycle shouldEqual ProbeInitializing
      underlying.health shouldEqual ProbeUnknown
      underlying.summary shouldEqual None
      underlying.lastChange shouldEqual None
      underlying.lastUpdate shouldEqual None
      underlying.correlationId shouldEqual None
      underlying.acknowledgementId shouldEqual None
      underlying.squelch shouldEqual false
    }

    "initialize and transition to running behavior" in {
      val ref = ProbeRef("fqdn:local/")
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.TestBehavior"
      val factory = ProbeBehavior.extensions(probeType).configure(Map.empty)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val probe = system.actorOf(Probe.props(ref, blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory, Set.empty, 0)

      val initializeProbeStatus = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeKnown, None, ProbeHealthy, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus, Some(status)))

      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))

      probe ! GetProbeStatus(ref)
      val getProbeStatusResult = expectMsgClass(classOf[GetProbeStatusResult])
      getProbeStatusResult.status.lifecycle shouldEqual ProbeKnown
      getProbeStatusResult.status.health shouldEqual ProbeHealthy
      getProbeStatusResult.status.summary shouldEqual None
      getProbeStatusResult.status.correlation shouldEqual None
      getProbeStatusResult.status.acknowledged shouldEqual None
      getProbeStatusResult.status.squelched shouldEqual false
    }

    "update behavior" in {
      val ref = ProbeRef("fqdn:local/")
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.TestBehavior"
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val factory1 = ProbeBehavior.extensions(probeType).configure(Map("key" -> "value1"))

      val probe = TestActorRef(new Probe(ref, blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory1, Set.empty, 0)

      val initializeProbeStatus1 = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status1 = ProbeStatus(DateTime.now(), ProbeKnown, None, ProbeHealthy, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus1, Some(status1)))

      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))

      probe.underlyingActor.processor shouldBe a [TestProcessor]
      probe.underlyingActor.processor should have ('properties (Map("key" -> "value1")))

      val factory2 = ProbeBehavior.extensions(probeType).configure(Map("key" -> "value2"))
      probe ! ChangeProbe(probeType, policy, factory2, Set.empty, 1)

      val initializeProbeStatus2 = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status2 = ProbeStatus(DateTime.now(), ProbeKnown, None, ProbeHealthy, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus2, Some(status2)))

      val updateProbeStatus2 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus2))

      probe.underlyingActor.processor shouldBe a [TestProcessor]
      probe.underlyingActor.processor should have ('properties (Map("key" -> "value2")))
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
      val policy = CheckPolicy(1.minute, 2.seconds, 1.minute, 1.minute, None)
      val probeType1 = "io.mandelbrot.core.system.TestBehavior"
      val factory1 = ProbeBehavior.extensions(probeType1).configure(Map.empty)

      val probe = TestActorRef(new Probe(ref, blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType1, policy, factory1, children, 0)

      val initializeProbeStatus1 = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status1 = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus1, Some(status1)))

      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))

      val probeType2 = "io.mandelbrot.core.system.TestChangeBehavior"
      val factory2 = ProbeBehavior.extensions(probeType2).configure(Map.empty)
      probe ! ChangeProbe(probeType2, policy, factory2, children, 1)

      val initializeProbeStatus2 = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status2 = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus2, Some(status2)))

      val updateProbeStatus2 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus2))

      probe.underlyingActor.children shouldEqual children
      probe.underlyingActor.policy shouldEqual policy
      probe.underlyingActor.processor shouldBe a [TestProcessorChange]
    }

    "transition to retired behavior" ignore {
      val ref = ProbeRef("fqdn:local/")
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.TestBehavior"
      val factory = ProbeBehavior.extensions(probeType).configure(Map.empty)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val probe = system.actorOf(Probe.props(ref, blackhole, services, metricsBus))
      watch(probe)
      probe ! ChangeProbe(probeType, policy, factory, Set.empty, 1)

      val initializeProbeStatus = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeKnown, None, ProbeHealthy, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus, Some(status)))

      probe ! RetireProbe(0)
      val deleteProbeStatus = stateService.expectMsgClass(classOf[DeleteProbeStatus])
      stateService.reply(DeleteProbeStatusResult(deleteProbeStatus))

      val result = expectMsgClass(classOf[Terminated])
      result.actor shouldEqual probe
    }
  }
}
