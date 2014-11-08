package io.mandelbrot.core.system

import akka.actor.{ActorSystem, Terminated}
import akka.testkit.{TestProbe, ImplicitSender, TestActorRef, TestKit}
import io.mandelbrot.core.metrics.MetricsBus
import org.joda.time.DateTime
import org.scalatest.matchers.MustMatchers
import org.scalatest.{WordSpecLike, BeforeAndAfterAll, WordSpec}
import scala.concurrent.duration._

import io.mandelbrot.core.registry.ProbePolicy
import io.mandelbrot.core.state._
import io.mandelbrot.core.{PersistenceConfig, AkkaConfig, Blackhole}
import io.mandelbrot.core.ConfigConversions._

class ProbeSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ProbeSpec", AkkaConfig ++ PersistenceConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val blackhole = system.actorOf(Blackhole.props())

  "A Probe" must {

    "have an initial state" in {
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val behavior = TestBehavior()
      val services = system.actorOf(TestServiceProxy.props())
      val metricsBus = new MetricsBus()
      val actor = TestActorRef(new Probe(ProbeRef("fqdn:local/"), blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val probe = actor.underlyingActor
      probe.lifecycle must be(ProbeInitializing)
      probe.health must be(ProbeUnknown)
      probe.summary must be(None)
      probe.lastChange must be(None)
      probe.lastUpdate must be(None)
      probe.correlationId must be(None)
      probe.acknowledgementId must be(None)
      probe.squelch must be(false)
    }

    "initialize and transition to running behavior" in {
      val ref = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val behavior = TestBehavior()
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      stateService.reply(InitializeProbeStateResult(initialize, status, 0))
      actor ! GetProbeStatus(ref)
      val result = expectMsgClass(classOf[GetProbeStatusResult])
      result.state.lifecycle must be(ProbeKnown)
      result.state.health must be(ProbeHealthy)
      result.state.summary must be(None)
      result.state.correlation must be(None)
      result.state.acknowledged must be(None)
      result.state.squelched must be(false)
    }

    "update behavior" in {
      val ref = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val behavior = TestUpdateBehavior(1)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      stateService.reply(InitializeProbeStateResult(initialize, status, 0))
      actor ! UpdateProbe(Set.empty, policy, TestUpdateBehavior(2), 1)
      val update = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update))
      actor ! GetProbeConfig(ref)
      val result = expectMsgClass(classOf[GetProbeConfigResult])
      result.behavior must be === TestUpdateBehavior(2)
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
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(InitializeProbeStateResult(initialize, status, 0))
      actor ! ChangeProbe(children, policy, TestChangeBehavior(), 1)
      val update = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update))
      actor ! GetProbeConfig(ref)
      val result = expectMsgClass(classOf[GetProbeConfigResult])
      result.children must be === children
      result.policy must be === policy
      result.behavior must be === TestChangeBehavior()
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
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      stateService.reply(InitializeProbeStateResult(initialize, status, 0))
      actor ! RetireProbe(0)
      val retire = stateService.expectMsgClass(classOf[DeleteProbeState])
      stateService.reply(DeleteProbeStateResult(retire))
      val result = expectMsgClass(classOf[Terminated])
      result.actor must be(actor)
    }
  }
}
