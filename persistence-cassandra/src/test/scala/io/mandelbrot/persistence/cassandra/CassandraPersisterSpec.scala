package io.mandelbrot.persistence.cassandra

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import io.mandelbrot.core.notification.NotifyHealthChanges
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpecLike}
import org.joda.time.{DateTimeZone, DateTime}

import io.mandelbrot.core.AkkaConfig
import io.mandelbrot.core.system._
import io.mandelbrot.core.state._
import io.mandelbrot.core.ConfigConversions._
import io.mandelbrot.persistence.cassandra.CassandraPersister.CassandraPersisterSettings

class CassandraPersisterSpec(_system: ActorSystem)
  extends TestKit(_system)
  with ImplicitSender
  with WordSpecLike
  with ShouldMatchers
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("ProbeSpec", AkkaConfig ++ CassandraConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  "CassandraPersister" should {

    val settings = CassandraPersisterSettings()
    val actor = system.actorOf(CassandraPersister.props(settings))

    "initialize probe status when probe doesn't exist" in {
      val probeRef = ProbeRef("test:1")
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val op = InitializeProbeStatus(probeRef, timestamp)
      actor ! op
      val initializeProbeStatusResult = expectMsgClass(classOf[InitializeProbeStatusResult])
      initializeProbeStatusResult.op shouldEqual op
      initializeProbeStatusResult.status shouldEqual None
    }

    "initialize probe status when probe exists" in {
      val probeRef = ProbeRef("test:2")
      val timestamp = DateTime.now(DateTimeZone.UTC)
      //val metrics = Map("metric" -> BigDecimal(0.1))
      val metrics = Map.empty[String,BigDecimal]
      val status = ProbeStatus(timestamp, ProbeKnown, Some("healthy"), ProbeHealthy, metrics, Some(timestamp),
        Some(timestamp), None, None, squelched = false)
      val notifications = Vector(NotifyHealthChanges(probeRef, timestamp, None, ProbeUnknown, ProbeHealthy))
      actor ! UpdateProbeStatus(probeRef, status, notifications, None)
      expectMsgClass(classOf[UpdateProbeStatusResult])
      val op = InitializeProbeStatus(probeRef, timestamp)
      actor ! op
      val initializeProbeStatusResult = expectMsgClass(classOf[InitializeProbeStatusResult])
      initializeProbeStatusResult.op shouldEqual op
      initializeProbeStatusResult.status shouldEqual Some(status)
    }

  }
}
