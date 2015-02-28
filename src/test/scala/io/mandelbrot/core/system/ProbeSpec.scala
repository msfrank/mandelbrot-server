package io.mandelbrot.core.system

import akka.actor.{ActorSystem, Terminated}
import akka.testkit.{TestProbe, ImplicitSender, TestActorRef, TestKit}
import io.mandelbrot.core.metrics.MetricsBus
import org.joda.time.DateTime
import org.scalatest.ShouldMatchers
import org.scalatest.{WordSpecLike, BeforeAndAfterAll}
import scala.concurrent.duration._

import io.mandelbrot.core.registry.ProbePolicy
import io.mandelbrot.core.state._
import io.mandelbrot.core.{PersistenceConfig, AkkaConfig, Blackhole}
import io.mandelbrot.core.ConfigConversions._

class ProbeSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ProbeSpec", AkkaConfig ++ PersistenceConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val blackhole = system.actorOf(Blackhole.props())

  "A Probe" should {

    "have an initial state" in {
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val behavior = TestBehavior()
      val services = system.actorOf(TestServiceProxy.props())
      val metricsBus = new MetricsBus()
      val actor = TestActorRef(new Probe(ProbeRef("fqdn:local/"), blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
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
      val behavior = TestBehavior()
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeKnown, None, ProbeHealthy, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initialize, status, 0))

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
      val behavior = TestUpdateBehavior(1)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeKnown, None, ProbeHealthy, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initialize, status, 0))

      actor ! UpdateProbe(Set.empty, policy, TestUpdateBehavior(2), 1)
      val update = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(update))

      actor ! GetProbeConfig(ref)
      val result = expectMsgClass(classOf[GetProbeConfigResult])
      result.behavior shouldEqual TestUpdateBehavior(2)
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
      val initialBehavior = TestBehavior()

      val actor = system.actorOf(Probe.props(ref, blackhole, children, policy, initialBehavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initialize, status, 0))

      actor ! ChangeProbe(children, policy, TestChangeBehavior(), 1)
      val update = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(update))

      actor ! GetProbeConfig(ref)
      val result = expectMsgClass(classOf[GetProbeConfigResult])
      result.children shouldEqual children
      result.policy shouldEqual policy
      result.behavior shouldEqual TestChangeBehavior()
    }

    "transition to retired behavior" in {
      val ref = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      watch(actor)

      val initialize = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeKnown, None, ProbeHealthy, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initialize, status, 0))

      actor ! RetireProbe(0)
      val retire = stateService.expectMsgClass(classOf[DeleteProbeStatus])
      stateService.reply(DeleteProbeStatusResult(retire))

      val result = expectMsgClass(classOf[Terminated])
      result.actor shouldEqual actor
    }
  }
}
