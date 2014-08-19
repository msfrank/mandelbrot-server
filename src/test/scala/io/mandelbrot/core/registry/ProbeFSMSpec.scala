package io.mandelbrot.core.registry

import akka.actor.ActorSystem
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import io.mandelbrot.core.message.StatusMessage
import io.mandelbrot.core.state.{ProbeStatusCommitted, ProbeState, InitializeProbeState}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import org.scalatest.matchers.MustMatchers
import scala.concurrent.duration._

import io.mandelbrot.core.Blackhole

import scala.util.Success

class ProbeFSMSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpec with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ProbeSpec", ConfigFactory.parseString(
    """
      |akka {
      |  loglevel = DEBUG
      |  loggers = ["akka.event.slf4j.Slf4jLogger"]
      |  debug {
      |    receive = on
      |    lifecycle = off
      |    fsm = off
      |    event-stream = on
      |    unhandled = on
      |    router-misconfiguration = on
      |  }
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

  val child1 = ProbeRef("fqdn:local/child1")
  val child2 = ProbeRef("fqdn:local/child2")
  val child3 = ProbeRef("fqdn:local/child3")
  val blackhole = system.actorOf(Blackhole.props())

  "A Probe implementing ProbeFSM" must {

    "transition behavior from scalar to aggregate" in {
      val ref = ProbeRef("fqdn:local/")
      val children = Set(child1, child2, child3)
      val stateService = new TestProbe(_system)

      val scalarPolicy = ProbePolicy(1.minute, 2.seconds, 1.minute, 1.minute, ScalarBehaviorPolicy(1.hour, 17), None)
      val aggregatePolicy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, AggregateBehaviorPolicy(alertOnAnyChild = false, 1.hour, 17), None)

      val probe = system.actorOf(Probe.props(ref, blackhole, children, scalarPolicy, 0, stateService.ref, blackhole, blackhole))
      stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(Success(ProbeState(status, 0)))

      probe ! StatusMessage(ref, ProbeHealthy, "healthy", None, DateTime.now())
      val result1 = stateService.expectMsgClass(classOf[ProbeState])
      result1.status.lifecycle must be(ProbeKnown)
      result1.status.health must be(ProbeHealthy)
      stateService.reply(Success(ProbeState(result1.status, 0)))

      probe ! ChangeProbe(children, aggregatePolicy, 1)
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

      val scalarPolicy = ProbePolicy(1.minute, 2.seconds, 1.minute, 1.minute, ScalarBehaviorPolicy(1.hour, 17), None)
      val aggregatePolicy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, AggregateBehaviorPolicy(alertOnAnyChild = false, 1.hour, 17), None)

      val probe = system.actorOf(Probe.props(ref, blackhole, children, aggregatePolicy, 0, stateService.ref, blackhole, blackhole))
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

      probe ! ChangeProbe(children, scalarPolicy, 1)
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