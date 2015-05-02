package io.mandelbrot.core.system

import akka.actor.ActorSystem
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import io.mandelbrot.core.registry.{CreateRegistrationResult, CreateRegistration}
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

  "A Agent" should {

    "register when it doesn't exist in the registry" in {

      val agentId = AgentId("test.1")
      val policy = CheckPolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
      val properties = Map.empty[String,String]
      val metadata = Map.empty[String,String]
      val check = CheckSpec("io.mandelbrot.core.system.ScalarCheck", policy, properties, metadata)
      val checks = Map(CheckId("load") -> check)
      val metrics = Map.empty[CheckId,Map[String,MetricSpec]]
      val registration = AgentRegistration(agentId, "mandelbrot", Map.empty, checks, metrics)

      val checkSystem = system.actorOf(Agent.props(services))

      checkSystem ! RegisterAgent(agentId, registration)
      val registerCheckSystemResult = expectMsgClass(classOf[RegisterCheckSystemResult])
      registerCheckSystemResult.metadata.lsn shouldEqual 0
    }

    "revive when it exists in the registry" in {

      val agentId = AgentId("test.2")
      val policy = CheckPolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
      val properties = Map.empty[String,String]
      val metadata = Map.empty[String,String]
      val check = CheckSpec("io.mandelbrot.core.system.ScalarCheck", policy, properties, metadata)
      val checks = Map(CheckId("load") -> check)
      val metrics = Map.empty[CheckId,Map[String,MetricSpec]]
      val registration = AgentRegistration(agentId, "mandelbrot", Map.empty, checks, metrics)

      services ! CreateRegistration(agentId, registration)
      expectMsgClass(classOf[CreateRegistrationResult])

      val checkSystem = system.actorOf(Agent.props(services))

      checkSystem ! ReviveAgent(agentId)

      checkSystem ! DescribeAgent(agentId)
      val describeCheckSystemResult = expectMsgClass(classOf[DescribeCheckSystemResult])
      describeCheckSystemResult.registration shouldEqual registration
      describeCheckSystemResult.lsn shouldEqual 0
    }

    "update checks when the registration changes" in {

      val agentId = AgentId("test.3")
      val policy = CheckPolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
      val properties = Map.empty[String,String]
      val metadata = Map.empty[String,String]
      val check1 = CheckSpec("io.mandelbrot.core.system.ScalarCheck", policy, properties, metadata)
      val checks1 = Map(CheckId("check1") -> check1)
      val metrics = Map.empty[CheckId,Map[String,MetricSpec]]
      val registration1 = AgentRegistration(agentId, "mandelbrot", Map.empty, checks1, metrics)

      val checkSystem = system.actorOf(Agent.props(services))

      checkSystem ! RegisterAgent(agentId, registration1)
      val registerCheckSystemResult = expectMsgClass(classOf[RegisterCheckSystemResult])

      val check2 = CheckSpec("io.mandelbrot.core.system.ScalarCheck", policy, properties, metadata)
      val checks2 = Map(CheckId("check2") -> check2)
      val registration2 = AgentRegistration(agentId, "mandelbrot", Map.empty, checks2, metrics)

      checkSystem ! UpdateAgent(agentId, registration2)
      val updateCheckSystemResult = expectMsgClass(classOf[UpdateCheckSystemResult])
      updateCheckSystemResult.metadata.lsn shouldEqual 1

      checkSystem ! GetCheckStatus(CheckRef("test.3:check2"))
      val getCheckStatusResult = expectMsgClass(classOf[GetCheckStatusResult])
    }
  }
}
