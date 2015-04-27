package io.mandelbrot.core.system

import java.net.URI

import akka.actor.ActorSystem
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import io.mandelbrot.core.entity.Entity
import io.mandelbrot.core.registry.{CreateRegistrationResult, CreateRegistration}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpecLike}
import scala.concurrent.duration._

import io.mandelbrot.core.{MandelbrotConfig, ServiceProxy, AkkaConfig}
import io.mandelbrot.core.model._
import io.mandelbrot.core.ConfigConversions._

class ProbeSystemSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ProbeSystemSpec", AkkaConfig ++ MandelbrotConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val services = system.actorOf(ServiceProxy.props(), "service-proxy")

  "A ProbeSystem" should {

    "register when it doesn't exist in the registry" in {

      val agentId = AgentId("test.1")
      val policy = CheckPolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
      val properties = Map.empty[String,String]
      val metadata = Map.empty[String,String]
      val probe = CheckSpec("io.mandelbrot.core.system.ScalarProbe", policy, properties, metadata)
      val probes = Map(CheckId("load") -> probe)
      val metrics = Map.empty[CheckId,Map[String,MetricSpec]]
      val registration = AgentRegistration(agentId, "mandelbrot", Map.empty, probes, metrics)

      val probeSystem = system.actorOf(ProbeSystem.props(services))

      probeSystem ! RegisterProbeSystem(agentId, registration)
      val registerProbeSystemResult = expectMsgClass(classOf[RegisterProbeSystemResult])
      registerProbeSystemResult.metadata.lsn shouldEqual 0
    }

    "revive when it exists in the registry" in {

      val agentId = AgentId("test.2")
      val policy = CheckPolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
      val properties = Map.empty[String,String]
      val metadata = Map.empty[String,String]
      val probe = CheckSpec("io.mandelbrot.core.system.ScalarProbe", policy, properties, metadata)
      val probes = Map(CheckId("load") -> probe)
      val metrics = Map.empty[CheckId,Map[String,MetricSpec]]
      val registration = AgentRegistration(agentId, "mandelbrot", Map.empty, probes, metrics)

      services ! CreateRegistration(agentId, registration)
      expectMsgClass(classOf[CreateRegistrationResult])

      val probeSystem = system.actorOf(ProbeSystem.props(services))

      probeSystem ! ReviveProbeSystem(agentId)

      probeSystem ! DescribeProbeSystem(agentId)
      val describeProbeSystemResult = expectMsgClass(classOf[DescribeProbeSystemResult])
      describeProbeSystemResult.registration shouldEqual registration
      describeProbeSystemResult.lsn shouldEqual 0
    }

    "update checks when the registration changes" in {

      val agentId = AgentId("test.3")
      val policy = CheckPolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
      val properties = Map.empty[String,String]
      val metadata = Map.empty[String,String]
      val probe1 = CheckSpec("io.mandelbrot.core.system.ScalarProbe", policy, properties, metadata)
      val probes1 = Map(CheckId("probe1") -> probe1)
      val metrics = Map.empty[CheckId,Map[String,MetricSpec]]
      val registration1 = AgentRegistration(agentId, "mandelbrot", Map.empty, probes1, metrics)

      val probeSystem = system.actorOf(ProbeSystem.props(services))

      probeSystem ! RegisterProbeSystem(agentId, registration1)
      val registerProbeSystemResult = expectMsgClass(classOf[RegisterProbeSystemResult])

      val probe2 = CheckSpec("io.mandelbrot.core.system.ScalarProbe", policy, properties, metadata)
      val probes2 = Map(CheckId("probe2") -> probe2)
      val registration2 = AgentRegistration(agentId, "mandelbrot", Map.empty, probes2, metrics)

      probeSystem ! UpdateProbeSystem(agentId, registration2)
      val updateProbeSystemResult = expectMsgClass(classOf[UpdateProbeSystemResult])
      updateProbeSystemResult.metadata.lsn shouldEqual 1

      probeSystem ! GetProbeStatus(ProbeRef("test.3:probe2"))
      val getProbeStatusResult = expectMsgClass(classOf[GetProbeStatusResult])
    }
  }
}
