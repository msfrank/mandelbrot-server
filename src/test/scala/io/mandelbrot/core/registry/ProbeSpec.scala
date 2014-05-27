package io.mandelbrot.core.registry

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import com.typesafe.config.ConfigFactory
import akka.testkit.{ImplicitSender, TestKit, TestActorRef}

import akka.actor.ActorSystem

class ProbeSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpec with MustMatchers {

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


  "A Probe" must {

    "have an initial state" in {
      val actor = TestActorRef(new Probe(ProbeRef("fqdn:local/"), self, self, self, self, self))
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
  }
}
