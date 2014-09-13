package io.mandelbrot.core.system

import akka.actor.{ActorSystem, Terminated}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime
import org.scalatest.matchers.MustMatchers
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import scala.concurrent.duration._
import scala.util.Success

import io.mandelbrot.core.registry.ProbePolicy
import io.mandelbrot.core.state.{InitializeProbeState, ProbeState}
import io.mandelbrot.core.{Blackhole, ServiceMap}

class ProbeSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpec with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ProbeSpec", ConfigFactory.parseString(
    """
      |akka {
      |  persistence {
      |    journal {
      |      plugin = "akka.persistence.journal.inmem"
      |      inmem {
      |        class = "akka.persistence.journal.inmem.InmemJournal"
      |        plugin-dispatcher = "akka.actor.default-dispatcher"
      |      }
      |    }
      |  }
      |}
    """.stripMargin)))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val blackhole = system.actorOf(Blackhole.props())

  "A Probe" must {

    "have an initial state" in {
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val services = ServiceMap(blackhole, blackhole, blackhole, blackhole, blackhole, blackhole)
      val actor = TestActorRef(new Probe(ProbeRef("fqdn:local/"), blackhole, Set.empty, policy, behavior, 0, services))
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
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services))
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
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services))
      watch(actor)
      expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      lastSender ! Success(ProbeState(status, 1))
      actor ! RetireProbe(1)
      val result = expectMsgClass(classOf[Terminated])
      result.actor must be(actor)
    }

  }
}
