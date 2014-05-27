package io.mandelbrot.core.registry

import org.scalatest.{BeforeAndAfterAll, WordSpec}
import org.scalatest.matchers.MustMatchers
import com.typesafe.config.ConfigFactory
import akka.testkit.{ImplicitSender, TestKit, TestActorRef}
import akka.actor.{ActorRef, ActorSystem}
import scala.concurrent.duration._

import io.mandelbrot.core.registry.ProbeSystem.InitProbe
import io.mandelbrot.core.notification.{EmitNotifications, NotificationPolicy}
import io.mandelbrot.core.message.StatusMessage
import org.joda.time.DateTime
import io.mandelbrot.core.Blackhole

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
      val actor = TestActorRef(new Probe(ProbeRef("fqdn:local/"), blackhole, blackhole, blackhole, blackhole, blackhole))
      val probe = actor.underlyingActor
      probe.lifecycle must be(ProbeJoining)
      probe.health must be(ProbeUnknown)
      probe.summary must be(None)
      probe.lastChange must be(None)
      probe.lastUpdate must be(None)
      probe.correlationId must be(None)
      probe.acknowledgementId must be(None)
      probe.squelch must be(false)
    }

    "reject all but InitProbe messages while in initializing behavior" in {
      //val actor = system.actorOf(Probe.props(ProbeRef("fqdn:local/"), self, self, self, self, self))
      //actor ! InitProbe()
    }

    "initialize and transition to running behavior and upon receiving InitProbe" in {
      val ref = ProbeRef("fqdn:local/")
      val actor = system.actorOf(Probe.props(ref, blackhole, blackhole, blackhole, blackhole, blackhole))
      val initialPolicy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, 1.hour, 17, NotificationPolicy(EmitNotifications, None))
      actor ! InitProbe(initialPolicy)
      actor ! GetProbeStatus(ref)
      val result = expectMsgClass(classOf[GetProbeStatusResult])
      result.state.lifecycle must be(ProbeJoining)
      result.state.health must be(ProbeUnknown)
      result.state.summary must be(None)
      result.state.correlation must be(None)
      result.state.acknowledged must be(None)
      result.state.squelched must be(false)
    }

    "transition from ProbeJoining/ProbeUnknown to ProbeKnown/ProbeHealthy when a healthy StatusMessage is received" in {
      val ref = ProbeRef("fqdn:local/")
      val actor = system.actorOf(Probe.props(ref, blackhole, blackhole, blackhole, blackhole, blackhole))
      val initialPolicy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, 1.hour, 17, NotificationPolicy(EmitNotifications, None))
      actor ! InitProbe(initialPolicy)
      val timestamp = DateTime.now()
      actor ! StatusMessage(ref, ProbeHealthy, "healthy", None, timestamp)
      actor ! GetProbeStatus(ref)
      val result = expectMsgClass(classOf[GetProbeStatusResult])
      result.state.lifecycle must be(ProbeKnown)
      result.state.health must be(ProbeHealthy)
      result.state.summary must be(Some("healthy"))
      result.state.correlation must be(None)
      result.state.acknowledged must be(None)
      result.state.squelched must be(false)
    }

    "transition from ProbeJoining/ProbeUnknown to ProbeKnown/ProbeDegraded when a degraded StatusMessage is received" in {
      val ref = ProbeRef("fqdn:local/")
      val actor = system.actorOf(Probe.props(ref, blackhole, blackhole, blackhole, blackhole, blackhole))
      val initialPolicy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, 1.hour, 17, NotificationPolicy(EmitNotifications, None))
      actor ! InitProbe(initialPolicy)
      val timestamp = DateTime.now()
      actor ! StatusMessage(ref, ProbeDegraded, "degraded", None, timestamp)
      actor ! GetProbeStatus(ref)
      val result = expectMsgClass(classOf[GetProbeStatusResult])
      result.state.lifecycle must be(ProbeKnown)
      result.state.health must be(ProbeDegraded)
      result.state.summary must be(Some("degraded"))
      result.state.correlation.isDefined must be(true)
      result.state.acknowledged.isEmpty must be(true)
      result.state.squelched must be(false)
    }

    "transition from ProbeJoining/ProbeUnknown to ProbeKnown/ProbeFailed when a failed StatusMessage is received" in {
      val ref = ProbeRef("fqdn:local/")
      val actor = system.actorOf(Probe.props(ref, blackhole, blackhole, blackhole, blackhole, blackhole))
      val initialPolicy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, 1.hour, 17, NotificationPolicy(EmitNotifications, None))
      actor ! InitProbe(initialPolicy)
      val timestamp = DateTime.now()
      actor ! StatusMessage(ref, ProbeFailed, "failed", None, timestamp)
      actor ! GetProbeStatus(ref)
      val result = expectMsgClass(classOf[GetProbeStatusResult])
      result.state.lifecycle must be(ProbeKnown)
      result.state.health must be(ProbeFailed)
      result.state.summary must be(Some("failed"))
      result.state.correlation.isDefined must be(true)
      result.state.acknowledged.isEmpty must be(true)
      result.state.squelched must be(false)
    }

    "transition from ProbeJoining/ProbeUnknown to ProbeKnown/ProbeUnknown when a unknown StatusMessage is received" in {
      val ref = ProbeRef("fqdn:local/")
      val actor = system.actorOf(Probe.props(ref, blackhole, blackhole, blackhole, blackhole, blackhole))
      val initialPolicy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, 1.hour, 17, NotificationPolicy(EmitNotifications, None))
      actor ! InitProbe(initialPolicy)
      val timestamp = DateTime.now()
      actor ! StatusMessage(ref, ProbeUnknown, "unknown", None, timestamp)
      actor ! GetProbeStatus(ref)
      val result = expectMsgClass(classOf[GetProbeStatusResult])
      result.state.lifecycle must be(ProbeKnown)
      result.state.health must be(ProbeUnknown)
      result.state.summary must be(Some("unknown"))
      result.state.correlation.isDefined must be(true)
      result.state.acknowledged.isEmpty must be(true)
      result.state.squelched must be(false)
    }



  }
}
