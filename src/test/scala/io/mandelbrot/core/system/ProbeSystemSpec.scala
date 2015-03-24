package io.mandelbrot.core.system

import java.net.URI

import akka.actor.ActorSystem
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import io.mandelbrot.core.entity.Entity
import io.mandelbrot.core.registry.{CreateProbeSystemEntryResult, CreateProbeSystemEntry}
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

      val uri = new URI("test:1")
      val policy = ProbePolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
      val properties = Map.empty[String,String]
      val metadata = Map.empty[String,String]
      val children = Map.empty[String,io.mandelbrot.core.model.ProbeSpec]
      val probe = ProbeSpec("io.mandelbrot.core.system.ScalarProbe", policy, properties, metadata, children)
      val probes = Map("load" -> probe)
      val metrics = Map.empty[MetricSource,MetricSpec]
      val registration = ProbeRegistration("io.mandelbrot.core.system.ProbeSystem", Map.empty, probes, metrics)

      val probeSystem = system.actorOf(ProbeSystem.props(services))

      probeSystem ! RegisterProbeSystem(uri, registration)
      val registerProbeSystemResult = expectMsgClass(classOf[RegisterProbeSystemResult])
      registerProbeSystemResult.lsn shouldEqual 0
    }

    "revive when it exists in the registry" in {

      val uri = new URI("test:2")
      val policy = ProbePolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
      val properties = Map.empty[String,String]
      val metadata = Map.empty[String,String]
      val children = Map.empty[String,io.mandelbrot.core.model.ProbeSpec]
      val probe = ProbeSpec("io.mandelbrot.core.system.ScalarProbe", policy, properties, metadata, children)
      val probes = Map("load" -> probe)
      val metrics = Map.empty[MetricSource,MetricSpec]
      val registration = ProbeRegistration("io.mandelbrot.core.system.ProbeSystem", Map.empty, probes, metrics)

      services ! CreateProbeSystemEntry(uri, registration)
      expectMsgClass(classOf[CreateProbeSystemEntryResult])

      val probeSystem = system.actorOf(ProbeSystem.props(services))

      probeSystem ! ReviveProbeSystem(uri)

      probeSystem ! DescribeProbeSystem(uri)
      val describeProbeSystemResult = expectMsgClass(classOf[DescribeProbeSystemResult])
      describeProbeSystemResult.registration shouldEqual registration
      describeProbeSystemResult.lsn shouldEqual 0
    }

    "update probes when the registration changes" in {

      val uri = new URI("test:3")
      val policy = ProbePolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
      val properties = Map.empty[String,String]
      val metadata = Map.empty[String,String]
      val children = Map.empty[String,io.mandelbrot.core.model.ProbeSpec]
      val probe1 = ProbeSpec("io.mandelbrot.core.system.ScalarProbe", policy, properties, metadata, children)
      val probes1 = Map("probe1" -> probe1)
      val metrics = Map.empty[MetricSource,MetricSpec]
      val registration1 = ProbeRegistration("io.mandelbrot.core.system.ProbeSystem", Map.empty, probes1, metrics)

      val probeSystem = system.actorOf(ProbeSystem.props(services))

      probeSystem ! RegisterProbeSystem(uri, registration1)
      val registerProbeSystemResult = expectMsgClass(classOf[RegisterProbeSystemResult])

      val probe2 = ProbeSpec("io.mandelbrot.core.system.ScalarProbe", policy, properties, metadata, children)
      val probes2 = Map("probe2" -> probe2)
      val registration2 = ProbeRegistration("io.mandelbrot.core.system.ProbeSystem", Map.empty, probes2, metrics)

      probeSystem ! UpdateProbeSystem(uri, registration2)
      val updateProbeSystemResult = expectMsgClass(classOf[UpdateProbeSystemResult])
      updateProbeSystemResult.lsn shouldEqual 1
    }
  }
}
