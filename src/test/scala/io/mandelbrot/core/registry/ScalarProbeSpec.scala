package io.mandelbrot.core.registry

import org.scalatest.{BeforeAndAfterAll, WordSpec}
import org.scalatest.matchers.MustMatchers
import com.typesafe.config.ConfigFactory
import akka.testkit.{TestProbe, ImplicitSender, TestKit, TestActorRef}
import akka.actor.{ActorRef, Actor, Terminated, ActorSystem}
import org.joda.time.DateTime
import scala.concurrent.duration._
import scala.util.Success

import io.mandelbrot.core.notification._
import io.mandelbrot.core.Blackhole
import io.mandelbrot.core.message.StatusMessage
import io.mandelbrot.core.state.{ProbeStatusCommitted, ProbeState, InitializeProbeState}

class ScalarProbeSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpec with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ScalarProbeSpec", ConfigFactory.parseString(
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

  "A Probe with scalar behavior" must {

    "transition to ProbeKnown/ProbeHealthy when a healthy StatusMessage is received" in {
      val ref = ProbeRef("fqdn:local/")
      val initialPolicy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, ScalarBehaviorPolicy(1.hour, 17), None)
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, initialPolicy, 0, self, blackhole, blackhole))
      expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeJoining, ProbeUnknown, None, None, None, None, None, false)
      lastSender ! Success(ProbeState(status, 0))
      val timestamp = DateTime.now()
      actor ! StatusMessage(ref, ProbeHealthy, "healthy", None, timestamp)
      val result = expectMsgClass(classOf[ProbeState])
      result.status.lifecycle must be(ProbeKnown)
      result.status.health must be(ProbeHealthy)
      result.status.summary must be(Some("healthy"))
      result.status.correlation must be(None)
      result.status.acknowledged must be(None)
      result.status.squelched must be(false)
    }

    "transition to ProbeKnown/ProbeDegraded when a degraded StatusMessage is received" in {
      val ref = ProbeRef("fqdn:local/")
      val initialPolicy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, ScalarBehaviorPolicy(1.hour, 17), None)
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, initialPolicy, 0, self, blackhole, blackhole))
      expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeJoining, ProbeUnknown, None, None, None, None, None, false)
      lastSender ! Success(ProbeState(status, 0))
      val timestamp = DateTime.now()
      actor ! StatusMessage(ref, ProbeDegraded, "degraded", None, timestamp)
      val result = expectMsgClass(classOf[ProbeState])
      result.status.lifecycle must be(ProbeKnown)
      result.status.health must be(ProbeDegraded)
      result.status.summary must be(Some("degraded"))
      result.status.correlation must not be(None)
      result.status.acknowledged must be(None)
      result.status.squelched must be(false)
    }

    "transition to ProbeKnown/ProbeFailed when a failed StatusMessage is received" in {
      val ref = ProbeRef("fqdn:local/")
      val initialPolicy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, ScalarBehaviorPolicy(1.hour, 17), None)
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, initialPolicy, 0, self, blackhole, blackhole))
      expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeJoining, ProbeUnknown, None, None, None, None, None, false)
      lastSender ! Success(ProbeState(status, 0))
      val timestamp = DateTime.now()
      actor ! StatusMessage(ref, ProbeFailed, "failed", None, timestamp)
      val result = expectMsgClass(classOf[ProbeState])
      result.status.lifecycle must be(ProbeKnown)
      result.status.health must be(ProbeFailed)
      result.status.summary must be(Some("failed"))
      result.status.correlation must not be(None)
      result.status.acknowledged must be(None)
      result.status.squelched must be(false)
    }

    "transition to ProbeKnown/ProbeUnknown when a unknown StatusMessage is received" in {
      val ref = ProbeRef("fqdn:local/")
      val initialPolicy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, ScalarBehaviorPolicy(1.hour, 17), None)
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, initialPolicy, 0, self, blackhole, blackhole))
      expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeJoining, ProbeUnknown, None, None, None, None, None, false)
      lastSender ! Success(ProbeState(status, 0))
      val timestamp = DateTime.now()
      actor ! StatusMessage(ref, ProbeUnknown, "unknown", None, timestamp)
      val result = expectMsgClass(classOf[ProbeState])
      result.status.lifecycle must be(ProbeKnown)
      result.status.health must be(ProbeUnknown)
      result.status.summary must be(Some("unknown"))
      result.status.correlation must not be(None)
      result.status.acknowledged must be(None)
      result.status.squelched must be(false)
    }

    "notify StateService when the joining timeout expires" in {
      val ref = ProbeRef("fqdn:local/")
      val initialPolicy = ProbePolicy(5.seconds, 1.minute, 1.minute, 1.minute, ScalarBehaviorPolicy(1.hour, 17), None)
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, initialPolicy, 0, self, blackhole, blackhole))
      expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeJoining, ProbeUnknown, None, None, None, None, None, false)
      lastSender ! Success(ProbeState(status, 0))
      // expiry timer should fire within 5 seconds
      within(10.seconds) {
        val result = expectMsgClass(classOf[ProbeState])
        result.status.probeRef must be(ref)
        result.status.lifecycle must be(ProbeJoining)
        result.status.health must be(ProbeUnknown)
        result.status.summary must be(None)
        result.status.correlation must not be(None)
        result.status.acknowledged must be(None)
        result.status.squelched must be(false)
      }
    }

    "notify StateService when the probe timeout expires" in {
      val ref = ProbeRef("fqdn:local/")
      val initialPolicy = ProbePolicy(1.minute, 2.seconds, 1.minute, 1.minute, ScalarBehaviorPolicy(1.hour, 17), None)
      val stateService = new TestProbe(_system)
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, initialPolicy, 0, stateService.ref, blackhole, blackhole))
      stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeJoining, ProbeUnknown, None, None, None, None, None, false)
      actor ! Success(ProbeState(status, 0))
      val timestamp = DateTime.now()
      actor ! StatusMessage(ref, ProbeHealthy, "healthy", None, timestamp)
      val state = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(state.status, state.lsn))
      // expiry timer should fire within 5 seconds
      val result = stateService.expectMsgClass(5.seconds, classOf[ProbeState])
      result.status.probeRef must be(ref)
      result.status.lifecycle must be(ProbeKnown)
      result.status.health must be(ProbeUnknown)
      result.status.summary must be(None)
      result.status.correlation must not be(None)
      result.status.acknowledged must be(None)
      result.status.squelched must be(false)
    }

    "notify NotificationService when the alert timeout expires" in {
      val ref = ProbeRef("fqdn:local/")
      val initialPolicy = ProbePolicy(1.minute, 1.minute, 2.seconds, 1.minute, ScalarBehaviorPolicy(1.hour, 17), None)
      val stateService = new TestProbe(_system)
      val notificationService = new TestProbe(_system)
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, initialPolicy, 0, stateService.ref, notificationService.ref, blackhole))
      stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeJoining, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(Success(ProbeState(status, 0)))
      val timestamp = DateTime.now()
      actor ! StatusMessage(ref, ProbeFailed, "failed", None, timestamp)
      val state = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(state.status, state.lsn))
      notificationService.expectMsgClass(classOf[NotifyLifecycleChanges])
      notificationService.expectMsgClass(classOf[NotifyHealthChanges])
      // expiry timer should fire within 5 seconds
      val notification = notificationService.expectMsgClass(5.seconds, classOf[NotifyHealthAlerts])
      notification.probeRef must be(ref)
      notification.health must be(ProbeFailed)
      notification.correlation must be === state.status.correlation
    }

  }
}

