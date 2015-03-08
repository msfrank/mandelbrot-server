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
    Cassandra(system).dropKeyspace()
    TestKit.shutdownActorSystem(system)
  }

  "CassandraPersister" should {

    val settings = CassandraPersisterSettings()
    val actor = system.actorOf(CassandraPersister.props(settings))

    "initialize probe status when probe doesn't exist" in {
      val probeRef = ProbeRef("test1")
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val op = InitializeProbeStatus(probeRef, timestamp)
      actor ! op
      val initializeProbeStatusResult = expectMsgClass(classOf[InitializeProbeStatusResult])
      initializeProbeStatusResult.op shouldEqual op
      initializeProbeStatusResult.status shouldEqual None
    }

    "initialize probe status when probe exists" in {
      val probeRef = ProbeRef("test2")
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val metrics = Map("metric" -> BigDecimal(0.1))
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

    "retrieve condition history" in {
      val probeRef = ProbeRef("test3")

      val status1 = ProbeStatus(DateTime.now(DateTimeZone.UTC), ProbeKnown, Some("healthy1"), ProbeHealthy,
        Map.empty, None, None, None, None, squelched = false)
      actor ! UpdateProbeStatus(probeRef, status1, Vector.empty, None)
      expectMsgClass(classOf[UpdateProbeStatusResult])
      val condition1 = ProbeCondition(status1.timestamp, status1.lifecycle, status1.summary, status1.health,
        status1.correlation, status1.acknowledged, status1.squelched)

      val status2 = ProbeStatus(DateTime.now(DateTimeZone.UTC), ProbeKnown, Some("healthy2"), ProbeHealthy,
        Map.empty, None, None, None, None, squelched = false)
      actor ! UpdateProbeStatus(probeRef, status2, Vector.empty, None)
      expectMsgClass(classOf[UpdateProbeStatusResult])
      val condition2 = ProbeCondition(status2.timestamp, status2.lifecycle, status2.summary, status2.health,
        status2.correlation, status2.acknowledged, status2.squelched)

      val status3 = ProbeStatus(DateTime.now(DateTimeZone.UTC), ProbeKnown, Some("healthy3"), ProbeHealthy,
        Map.empty, None, None, None, None, squelched = false)
      actor ! UpdateProbeStatus(probeRef, status3, Vector.empty, None)
      expectMsgClass(classOf[UpdateProbeStatusResult])
      val condition3 = ProbeCondition(status3.timestamp, status3.lifecycle, status3.summary, status3.health,
        status3.correlation, status3.acknowledged, status3.squelched)

      actor ! GetConditionHistory(probeRef, None, None, Some(100), None)
      val getConditionHistoryResult = expectMsgClass(classOf[GetConditionHistoryResult])
      getConditionHistoryResult.history shouldEqual Vector(condition1, condition2, condition3)
      getConditionHistoryResult.exhausted shouldEqual true
    }
  }
}
