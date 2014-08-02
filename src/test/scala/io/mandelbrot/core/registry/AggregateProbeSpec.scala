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

class AggregateProbeSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpec with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("AggregateProbeSpec", ConfigFactory.parseString(
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

  val child1 = ProbeRef("fqdn:local/child1")
  val child2 = ProbeRef("fqdn:local/child2")
  val child3 = ProbeRef("fqdn:local/child3")
  val blackhole = system.actorOf(Blackhole.props())

  "A Probe with aggregate behavior" must {

    "transition to ProbeKnown/ProbeHealthy when all children have notified" in {
      val ref = ProbeRef("fqdn:local/")
      val behavior = AggregateBehaviorPolicy(alertOnAnyChild = false, 1.hour, 17)
      val initialPolicy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, behavior, None)
      val children = Set(child1, child2, child3)
      val stateService = new TestProbe(_system)
      val probe = system.actorOf(Probe.props(ref, blackhole, children, initialPolicy, 0, stateService.ref, blackhole, blackhole))
      stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeJoining, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(Success(ProbeState(status, 0)))
      val timestamp = DateTime.now()
      probe ! ProbeStatus(child1, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      probe ! ProbeStatus(child2, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      probe ! ProbeStatus(child3, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      val result = stateService.expectMsgClass(classOf[ProbeState])
      println(result)
      result.status.lifecycle must be(ProbeKnown)
      result.status.health must be(ProbeHealthy)
      result.status.correlation must be(None)
      result.status.acknowledged must be(None)
    }

  }
}
