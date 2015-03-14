package io.mandelbrot.persistence.cassandra

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpecLike}
import org.joda.time.{DateTimeZone, DateTime}

import io.mandelbrot.core.AkkaConfig
import io.mandelbrot.core.model._
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

  val settings = CassandraPersisterSettings()
  val actor = system.actorOf(CassandraPersister.props(settings))

  "A CassandraPersister" should {

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
  }

  "A CassandraPersister containing probe status within a single epoch" should {

    val today = new DateTime(DateTimeZone.UTC).toDateMidnight

    val probeRef = ProbeRef("test3")

    val timestamp1 = today.toDateTime.plusMinutes(1)
    val status1 = ProbeStatus(timestamp1, ProbeKnown, Some("healthy1"), ProbeHealthy,
      Map.empty, None, None, None, None, squelched = false)
    actor ! UpdateProbeStatus(probeRef, status1, Vector.empty, None)
    expectMsgClass(classOf[UpdateProbeStatusResult])
    val condition1 = ProbeCondition(status1.timestamp, status1.lifecycle, status1.summary, status1.health,
      status1.correlation, status1.acknowledged, status1.squelched)

    val timestamp2 = today.toDateTime.plusMinutes(2)
    val status2 = ProbeStatus(timestamp2, ProbeKnown, Some("healthy2"), ProbeHealthy,
      Map.empty, None, None, None, None, squelched = false)
    actor ! UpdateProbeStatus(probeRef, status2, Vector.empty, None)
    expectMsgClass(classOf[UpdateProbeStatusResult])
    val condition2 = ProbeCondition(status2.timestamp, status2.lifecycle, status2.summary, status2.health,
      status2.correlation, status2.acknowledged, status2.squelched)

    val timestamp3 = today.toDateTime.plusMinutes(3)
    val status3 = ProbeStatus(timestamp3, ProbeKnown, Some("healthy3"), ProbeHealthy,
      Map.empty, None, None, None, None, squelched = false)
    actor ! UpdateProbeStatus(probeRef, status3, Vector.empty, None)
    expectMsgClass(classOf[UpdateProbeStatusResult])
    val condition3 = ProbeCondition(status3.timestamp, status3.lifecycle, status3.summary, status3.health,
      status3.correlation, status3.acknowledged, status3.squelched)

    val timestamp4 = today.toDateTime.plusMinutes(4)
    val status4 = ProbeStatus(timestamp4, ProbeKnown, Some("healthy4"), ProbeHealthy,
      Map.empty, None, None, None, None, squelched = false)
    actor ! UpdateProbeStatus(probeRef, status4, Vector.empty, None)
    expectMsgClass(classOf[UpdateProbeStatusResult])
    val condition4 = ProbeCondition(status4.timestamp, status4.lifecycle, status4.summary, status4.health,
      status4.correlation, status4.acknowledged, status4.squelched)

    val timestamp5 = today.toDateTime.plusMinutes(5)
    val status5 = ProbeStatus(timestamp5, ProbeKnown, Some("healthy3"), ProbeHealthy,
      Map.empty, None, None, None, None, squelched = false)
    actor ! UpdateProbeStatus(probeRef, status5, Vector.empty, None)
    expectMsgClass(classOf[UpdateProbeStatusResult])
    val condition5 = ProbeCondition(status5.timestamp, status5.lifecycle, status5.summary, status5.health,
      status5.correlation, status5.acknowledged, status5.squelched)

    "retrieve condition history with no windowing parameters" in {
      actor ! GetConditionHistory(probeRef, None, None, Some(100), None)
      val getConditionHistoryResult = expectMsgClass(classOf[GetConditionHistoryResult])
      getConditionHistoryResult.page.history shouldEqual Vector(condition1, condition2, condition3, condition4, condition5)
      getConditionHistoryResult.page.exhausted shouldEqual true
    }

    "retrieve condition history with from specified" in {
      actor ! GetConditionHistory(probeRef, Some(timestamp3), None, Some(100), None)
      val getConditionHistoryResult = expectMsgClass(classOf[GetConditionHistoryResult])
      getConditionHistoryResult.page.history shouldEqual Vector(condition3, condition4, condition5)
      getConditionHistoryResult.page.exhausted shouldEqual true
    }

    "retrieve condition history with to specified" in {
      actor ! GetConditionHistory(probeRef, None, Some(timestamp4), Some(100), None)
      val getConditionHistoryResult = expectMsgClass(classOf[GetConditionHistoryResult])
      getConditionHistoryResult.page.history shouldEqual Vector(condition1, condition2, condition3)
      getConditionHistoryResult.page.exhausted shouldEqual true
    }

    "retrieve condition history with from and to specified" in {
      actor ! GetConditionHistory(probeRef, Some(timestamp2), Some(timestamp5), Some(100), None)
      val getConditionHistoryResult = expectMsgClass(classOf[GetConditionHistoryResult])
      getConditionHistoryResult.page.history shouldEqual Vector(condition2, condition3, condition4)
      getConditionHistoryResult.page.exhausted shouldEqual true
    }
  }
}
