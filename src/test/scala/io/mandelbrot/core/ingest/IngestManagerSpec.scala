package io.mandelbrot.core.ingest

import akka.actor.{PoisonPill, ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import io.mandelbrot.core.model.{ScalarMapObservation, ProbeRef}
import io.mandelbrot.core.util.Timestamp
import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpecLike}

import io.mandelbrot.core.{ServerConfig, AkkaConfig, MandelbrotConfig}
import io.mandelbrot.core.ConfigConversions._

class IngestManagerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("IngestManagerSpec", AkkaConfig ++ MandelbrotConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val settings = ServerConfig(system).settings

  def withIngestService(testCode: (ActorRef) => Any) {
    val ingestService = system.actorOf(IngestManager.props(settings.ingest, settings.cluster.enabled))
    testCode(ingestService)
    ingestService ! PoisonPill
  }

  val probeRef = ProbeRef("foo.agent:system.load")

  "An IngestManager" when {

    "servicing an AppendObservation request" should {

      "append an observation" in withIngestService { ingestService =>
        val observation = ScalarMapObservation(DateTime.now(DateTimeZone.UTC), Map("load1" -> 1.0))
        val timestamp = Timestamp()
        ingestService ! AppendObservation(probeRef, timestamp, observation)
        val appendObservationResult = expectMsgClass(classOf[AppendObservationResult])
      }
    }

    "servicing a GetObservations request" should {

    }

    "servicing a GetPartitions request" should {

    }

    "servicing a PutCheckpoint request" should {

    }
  }
}
