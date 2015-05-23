package io.mandelbrot.core.system

import akka.actor.ActorSystem
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import io.mandelbrot.core.registry.{CreateRegistrationResult, CreateRegistration}
import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpecLike}
import scala.concurrent.duration._

import io.mandelbrot.core.entity.Entity
import io.mandelbrot.core.{MandelbrotConfig, ServiceProxy, AkkaConfig}
import io.mandelbrot.core.model._
import io.mandelbrot.core.ConfigConversions._

class AgentSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("AgentSpec", AkkaConfig ++ MandelbrotConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val services = system.actorOf(ServiceProxy.props(), "service-proxy")

  "An Agent" should {

    "register when it doesn't exist in the registry" in {

      val agentId = AgentId("test.1")
      val policy = CheckPolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
      val check = CheckSpec("io.mandelbrot.core.system.ScalarCheck", policy, Map.empty, Map.empty)
      val checks = Map(CheckId("load") -> check)
      val metrics = Map.empty[CheckId,Map[String,MetricSpec]]
      val registration = AgentRegistration(agentId, "mandelbrot", Map.empty, checks, metrics)

      val agent = system.actorOf(Agent.props(services))

      agent ! RegisterAgent(agentId, registration)
      val registerAgentResult = expectMsgClass(classOf[RegisterAgentResult])
      registerAgentResult.metadata.generation shouldEqual 1
    }

    "revive when it exists in the registry" in {

      val agentId = AgentId("test.2")
      val policy = CheckPolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
      val check = CheckSpec("io.mandelbrot.core.system.ScalarCheck", policy, Map.empty, Map.empty)
      val checks = Map(CheckId("load") -> check)
      val metrics = Map.empty[CheckId,Map[String,MetricSpec]]
      val registration = AgentRegistration(agentId, "mandelbrot", Map.empty, checks, metrics)
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val metadata = AgentMetadata(agentId, 1, timestamp, timestamp, None)

      services ! CreateRegistration(agentId, registration, metadata, 1)
      expectMsgClass(classOf[CreateRegistrationResult])

      val agent = system.actorOf(Agent.props(services))

      agent ! ReviveAgent(agentId)

      agent ! DescribeAgent(agentId)
      val describeAgentResult = expectMsgClass(classOf[DescribeAgentResult])
      describeAgentResult.registration shouldEqual registration
      describeAgentResult.generation shouldEqual 1
    }

    "update checks when the registration changes" in {

      val agentId = AgentId("test.3")
      val policy = CheckPolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
      val check1 = CheckSpec("io.mandelbrot.core.system.ScalarCheck", policy, Map.empty, Map.empty)
      val checks1 = Map(CheckId("check1") -> check1)
      val metrics = Map.empty[CheckId,Map[String,MetricSpec]]
      val registration1 = AgentRegistration(agentId, "mandelbrot", Map.empty, checks1, metrics)

      val agent = system.actorOf(Agent.props(services))

      agent ! RegisterAgent(agentId, registration1)
      val registerAgentResult = expectMsgClass(classOf[RegisterAgentResult])

      val check2 = CheckSpec("io.mandelbrot.core.system.ScalarCheck", policy, Map.empty, Map.empty)
      val checks2 = Map(CheckId("check2") -> check2)
      val registration2 = AgentRegistration(agentId, "mandelbrot", Map.empty, checks2, metrics)

      agent ! UpdateAgent(agentId, registration2)
      val updateAgentResult = expectMsgClass(classOf[UpdateAgentResult])
      updateAgentResult.metadata.generation shouldEqual 1

      agent ! GetCheckStatus(CheckRef("test.3:check2"))
      val getCheckStatusResult = expectMsgClass(classOf[GetCheckStatusResult])
    }
  }
}
