package io.mandelbrot.core.agent

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.testkit.{ImplicitSender, TestKit}
import io.mandelbrot.core.ConfigConversions._
import io.mandelbrot.core.model._
import io.mandelbrot.core.registry.{CommitRegistration, CommitRegistrationResult}
import io.mandelbrot.core.check.{GetCheckStatus, GetCheckStatusResult}
import io.mandelbrot.core.{AkkaConfig, MandelbrotConfig, ServiceProxy}
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpecLike}
import spray.json.{JsString, JsObject}

import scala.concurrent.duration._

class AgentSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("AgentSpec", AkkaConfig ++ MandelbrotConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  def withServiceProxy(testCode: (ActorRef) => Any) {
    val services = system.actorOf(ServiceProxy.props())
    testCode(services)
    services ! PoisonPill
  }

  //val services = system.actorOf(ServiceProxy.props(), "service-proxy")

  "An Agent" should {

    "register when it doesn't exist in the registry" in withServiceProxy { services =>

      val agentId = AgentId("test.1")
      val probePolicy = ProbePolicy(PerMinute)
      val probes = Map(ProbeId("load") -> ProbeSpec(probePolicy, Map("load1" -> MetricSpec(GaugeSource, Units))))
      val checkPolicy = CheckPolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
      val checkSpec = CheckSpec("io.mandelbrot.core.check.TimeseriesCheck", checkPolicy,
        Some(JsObject("evaluation" -> JsString("probe:load:load1 > 1"))))
      val checks = Map(CheckId("load") -> checkSpec)
      val agentPolicy = AgentPolicy(5.seconds)
      val registration = AgentSpec(agentId, "mandelbrot", agentPolicy, probes, checks)

      val agent = system.actorOf(Agent.props(services))

      agent ! RegisterAgent(agentId, registration)
      val registerAgentResult = expectMsgClass(classOf[RegisterAgentResult])
      registerAgentResult.metadata.generation shouldEqual 1
    }

    "revive when it exists in the registry" in withServiceProxy { services =>

      val agentId = AgentId("test.2")
      val probePolicy = ProbePolicy(PerMinute)
      val probes = Map(ProbeId("load") -> ProbeSpec(probePolicy, Map("load1" -> MetricSpec(GaugeSource, Units))))
      val checkPolicy = CheckPolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
      val checkSpec = CheckSpec("io.mandelbrot.core.check.TimeseriesCheck", checkPolicy,
        Some(JsObject("evaluation" -> JsString("probe:load:load1 > 1"))))
      val checks = Map(CheckId("load") -> checkSpec)
      val agentPolicy = AgentPolicy(5.seconds)
      val registration = AgentSpec(agentId, "mandelbrot", agentPolicy, probes, checks)
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val metadata = AgentMetadata(agentId, 1, timestamp, timestamp, None)

      services ! CommitRegistration(agentId, registration, metadata, 1)
      expectMsgClass(classOf[CommitRegistrationResult])

      val agent = system.actorOf(Agent.props(services))

      agent ! ReviveAgent(agentId)

      agent ! DescribeAgent(agentId)
      val describeAgentResult = expectMsgClass(classOf[DescribeAgentResult])
      describeAgentResult.registration shouldEqual registration
      describeAgentResult.metadata shouldEqual metadata
    }

    "update checks when the registration changes" in withServiceProxy { services =>

      val agentId = AgentId("test.3")
      val probePolicy = ProbePolicy(PerMinute)
      val probes = Map(ProbeId("load") -> ProbeSpec(probePolicy, Map("load1" -> MetricSpec(GaugeSource, Units))))
      val checkPolicy = CheckPolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
      val checkSpec = CheckSpec("io.mandelbrot.core.check.TimeseriesCheck", checkPolicy,
        Some(JsObject("evaluation" -> JsString("probe:load:load1 > 1"))))
      val checks1 = Map(CheckId("check1") -> checkSpec)
      val agentPolicy = AgentPolicy(5.seconds)
      val registration1 = AgentSpec(agentId, "mandelbrot", agentPolicy, probes, checks1)

      val agent = system.actorOf(Agent.props(services))

      agent ! RegisterAgent(agentId, registration1)
      val registerAgentResult = expectMsgClass(classOf[RegisterAgentResult])

      val checks2 = Map(CheckId("check2") -> checkSpec)
      val registration2 = AgentSpec(agentId, "mandelbrot", agentPolicy, probes, checks2)

      agent ! UpdateAgent(agentId, registration2)
      val updateAgentResult = expectMsgClass(classOf[UpdateAgentResult])
      updateAgentResult.metadata.generation shouldEqual 1

      agent ! GetCheckStatus(CheckRef("test.3:check2"))
      val getCheckStatusResult = expectMsgClass(classOf[GetCheckStatusResult])
    }

    "mark registration and stop checks when retiring" in withServiceProxy { services =>

      val agentId = AgentId("test.4")
      val probePolicy = ProbePolicy(PerMinute)
      val probes = Map(ProbeId("load") -> ProbeSpec(probePolicy, Map("load1" -> MetricSpec(GaugeSource, Units))))
      val checkPolicy = CheckPolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
      val checkSpec = CheckSpec("io.mandelbrot.core.check.TimeseriesCheck", checkPolicy,
        Some(JsObject("evaluation" -> JsString("probe:load:load1 > 1"))))
      val checks = Map(CheckId("check1") -> checkSpec)
      val agentPolicy = AgentPolicy(5.seconds)
      val registration1 = AgentSpec(agentId, "mandelbrot", agentPolicy, probes, checks)

      val agent = system.actorOf(Agent.props(services))

      agent ! RegisterAgent(agentId, registration1)
      val registerAgentResult = expectMsgClass(classOf[RegisterAgentResult])

      agent ! DescribeAgent(AgentId("test.4"))
      val describeAgentResult1 = expectMsgClass(classOf[DescribeAgentResult])
      describeAgentResult1.registration shouldEqual registration1

      agent ! RetireAgent(agentId)
      val retireAgentResult = expectMsgClass(classOf[RetireAgentResult])
      retireAgentResult.metadata.expires.nonEmpty shouldEqual true

      agent ! DescribeAgent(AgentId("test.4"))
      val describeAgentResult2 = expectMsgClass(classOf[DescribeAgentResult])
      describeAgentResult2.registration shouldEqual registration1
      describeAgentResult2.metadata shouldEqual retireAgentResult.metadata
    }
  }
}
