package io.mandelbrot.core.system

import akka.actor.{ActorSystem, Terminated}
import akka.testkit.{TestProbe, ImplicitSender, TestActorRef, TestKit}
import io.mandelbrot.core.metrics.MetricsBus
import org.joda.time.DateTime
import org.scalatest.matchers.MustMatchers
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import scala.concurrent.duration._
import scala.util.Success

import io.mandelbrot.core.registry.ProbePolicy
import io.mandelbrot.core.state.{ProbeStatusCommitted, InitializeProbeState, ProbeState}
import io.mandelbrot.core.{PersistenceConfig, AkkaConfig, Blackhole, ServiceMap}
import io.mandelbrot.core.ConfigConversions._

class ProbeSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpec with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ProbeSpec", AkkaConfig ++ PersistenceConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val blackhole = system.actorOf(Blackhole.props())
  val child1 = ProbeRef("fqdn:local/child1")
  val child2 = ProbeRef("fqdn:local/child2")
  val child3 = ProbeRef("fqdn:local/child3")

  "A Probe" must {

    "have an initial state" in {
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val services = ServiceMap(blackhole, blackhole, blackhole, blackhole, blackhole, blackhole)
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
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val services = ServiceMap(blackhole, blackhole, blackhole, blackhole, self, blackhole)
      val metricsBus = new MetricsBus()
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      lastSender ! Success(ProbeState(status, 0))
      actor ! GetProbeStatus(ref)
      val result = expectMsgClass(classOf[GetProbeStatusResult])
      result.state.lifecycle must be(ProbeKnown)
      result.state.health must be(ProbeHealthy)
      result.state.summary must be(None)
      result.state.correlation must be(None)
      result.state.acknowledged must be(None)
      result.state.squelched must be(false)
    }

    "initialize and transition to retired behavior if lsn is newer than generation" in {
      val ref = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val services = ServiceMap(blackhole, blackhole, blackhole, blackhole, self, blackhole)
      val metricsBus = new MetricsBus()
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      watch(actor)
      expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      lastSender ! Success(ProbeState(status, 1))
      actor ! RetireProbe(1)
      val result = expectMsgClass(classOf[Terminated])
      result.actor must be(actor)
    }


    "transition behavior from scalar to aggregate" in {
      val ref = ProbeRef("fqdn:local/")
      val children = Set(child1, child2, child3)
      val stateService = new TestProbe(_system)
      val services = ServiceMap(blackhole, blackhole, blackhole, blackhole, stateService.ref, blackhole)
      val metricsBus = new MetricsBus()

      val policy = ProbePolicy(1.minute, 2.seconds, 1.minute, 1.minute, None)
      val scalarBehavior = ScalarProbeBehavior(1.hour, 17)
      val aggregateBehavior = AggregateProbeBehavior(EvaluateWorst, 1.hour, 17)

      val probe = system.actorOf(Probe.props(ref, blackhole, children, policy, scalarBehavior, 0, services, metricsBus))
      stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(Success(ProbeState(status, 0)))

      probe ! StatusMessage(ref, ProbeHealthy, "healthy", None, DateTime.now())
      val result1 = stateService.expectMsgClass(classOf[ProbeState])
      result1.status.lifecycle must be(ProbeKnown)
      result1.status.health must be(ProbeHealthy)
      stateService.reply(Success(ProbeState(result1.status, 0)))

      probe ! ChangeProbe(children, policy, aggregateBehavior, 1)
      val result2 = stateService.expectMsgClass(classOf[ProbeState])
      result2.status.lifecycle must be(ProbeInitializing)
      result2.status.health must be(ProbeUnknown)
      stateService.reply(Success(ProbeStatusCommitted(result2.status, 1)))

      val timestamp = DateTime.now()
      probe ! ProbeStatus(child1, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      probe ! ProbeStatus(child2, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      probe ! ProbeStatus(child3, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      val result3 = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(Success(ProbeState(result3.status, 1)))
      result3.status.lifecycle must be(ProbeSynthetic)
      result3.status.health must be(ProbeHealthy)
    }

    "transition behavior from aggregate to scalar" in {
      val ref = ProbeRef("fqdn:local/")
      val children = Set(child1, child2, child3)
      val stateService = new TestProbe(_system)
      val services = ServiceMap(blackhole, blackhole, blackhole, blackhole, stateService.ref, blackhole)
      val metricsBus = new MetricsBus()

      val policy = ProbePolicy(1.minute, 2.seconds, 1.minute, 1.minute, None)
      val aggregateBehavior = AggregateProbeBehavior(EvaluateWorst, 1.hour, 17)
      val scalarBehavior = ScalarProbeBehavior(1.hour, 17)

      val probe = system.actorOf(Probe.props(ref, blackhole, children, policy, aggregateBehavior, 0, services, metricsBus))
      stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(Success(ProbeState(status, 0)))

      val timestamp = DateTime.now()
      probe ! ProbeStatus(child1, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      probe ! ProbeStatus(child2, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      probe ! ProbeStatus(child3, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      val result1 = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(Success(ProbeState(result1.status, 1)))
      result1.status.lifecycle must be(ProbeSynthetic)
      result1.status.health must be(ProbeHealthy)

      probe ! ChangeProbe(children, policy, scalarBehavior, 1)
      val result2 = stateService.expectMsgClass(classOf[ProbeState])
      result2.status.lifecycle must be(ProbeInitializing)
      result2.status.health must be(ProbeUnknown)
      stateService.reply(Success(ProbeStatusCommitted(result2.status, 1)))

      probe ! StatusMessage(ref, ProbeHealthy, "healthy", None, DateTime.now())
      val result3 = stateService.expectMsgClass(classOf[ProbeState])
      result3.status.lifecycle must be(ProbeKnown)
      result3.status.health must be(ProbeHealthy)
      stateService.reply(Success(ProbeState(result3.status, 0)))
    }
  }
}