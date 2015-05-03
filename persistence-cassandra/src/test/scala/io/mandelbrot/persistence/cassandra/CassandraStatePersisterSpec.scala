package io.mandelbrot.persistence.cassandra

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpecLike}
import org.joda.time.{DateTimeZone, DateTime}

import io.mandelbrot.core.AkkaConfig
import io.mandelbrot.core.model._
import io.mandelbrot.core.state._
import io.mandelbrot.core.ConfigConversions._

class CassandraStatePersisterSpec(_system: ActorSystem)
  extends TestKit(_system)
  with ImplicitSender
  with WordSpecLike
  with ShouldMatchers
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("CheckSpec", AkkaConfig ++ CassandraConfig))

  // shutdown the actor system
  override def afterAll() {
    Cassandra(system).dropKeyspace()
    TestKit.shutdownActorSystem(system)
  }

  val settings = CassandraStatePersisterSettings()
  val actor = system.actorOf(CassandraStatePersister.props(settings))

  "A CassandraStatePersister" should {

    "initialize check status when check doesn't exist" in {
      val checkRef = CheckRef("foo.local.1:check")
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val op = InitializeCheckStatus(checkRef, timestamp)
      actor ! op
      val initializeCheckStatusResult = expectMsgClass(classOf[InitializeCheckStatusResult])
      initializeCheckStatusResult.op shouldEqual op
      initializeCheckStatusResult.status shouldEqual None
    }

    "initialize check status when check exists" in {
      val checkRef = CheckRef("foo.local.2:check")
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val metrics = Map("metric" -> BigDecimal(0.1))
      val status = CheckStatus(timestamp, CheckKnown, Some("healthy"), CheckHealthy, metrics, Some(timestamp),
        Some(timestamp), None, None, squelched = false)
      val notifications = Vector(NotifyHealthChanges(checkRef, timestamp, None, CheckUnknown, CheckHealthy))
      actor ! UpdateCheckStatus(checkRef, status, notifications, None)
      expectMsgClass(classOf[UpdateCheckStatusResult])
      val op = InitializeCheckStatus(checkRef, timestamp)
      actor ! op
      val initializeCheckStatusResult = expectMsgClass(classOf[InitializeCheckStatusResult])
      initializeCheckStatusResult.op shouldEqual op
      initializeCheckStatusResult.status shouldEqual Some(status)
    }
  }

  "A CassandraStatePersister containing check status within a single epoch" should {

    val today = new DateTime(DateTimeZone.UTC).toDateMidnight

    val checkRef = CheckRef("foo.local.3:check")

    val timestamp1 = today.toDateTime.plusMinutes(1)
    val metrics1 = CheckMetrics(timestamp1, Map("load" -> BigDecimal(1)))
    val status1 = CheckStatus(timestamp1, CheckKnown, Some("healthy1"), CheckHealthy,
      metrics1.metrics, None, None, None, None, squelched = false)
    val notifications1 = CheckNotifications(timestamp1, Vector(NotifyLifecycleChanges(checkRef, timestamp1, CheckJoining, CheckKnown)))
    actor ! UpdateCheckStatus(checkRef, status1, notifications1.notifications, None)
    expectMsgClass(classOf[UpdateCheckStatusResult])
    val condition1 = CheckCondition(status1.timestamp, status1.lifecycle, status1.summary, status1.health,
      status1.correlation, status1.acknowledged, status1.squelched)

    val timestamp2 = today.toDateTime.plusMinutes(2)
    val metrics2 = CheckMetrics(timestamp2, Map("load" -> BigDecimal(2)))
    val status2 = CheckStatus(timestamp2, CheckKnown, Some("healthy2"), CheckHealthy,
      metrics2.metrics, None, None, None, None, squelched = false)
    val notifications2 = CheckNotifications(timestamp2, Vector(NotifyLifecycleChanges(checkRef, timestamp1, CheckJoining, CheckKnown)))
    actor ! UpdateCheckStatus(checkRef, status2, notifications2.notifications, Some(timestamp1))
    expectMsgClass(classOf[UpdateCheckStatusResult])
    val condition2 = CheckCondition(status2.timestamp, status2.lifecycle, status2.summary, status2.health,
      status2.correlation, status2.acknowledged, status2.squelched)

    val timestamp3 = today.toDateTime.plusMinutes(3)
    val metrics3 = CheckMetrics(timestamp3, Map("load" -> BigDecimal(3)))
    val status3 = CheckStatus(timestamp3, CheckKnown, Some("healthy3"), CheckHealthy,
      metrics3.metrics, None, None, None, None, squelched = false)
    val notifications3 = CheckNotifications(timestamp3, Vector(NotifyLifecycleChanges(checkRef, timestamp1, CheckJoining, CheckKnown)))
    actor ! UpdateCheckStatus(checkRef, status3, notifications3.notifications, Some(timestamp2))
    expectMsgClass(classOf[UpdateCheckStatusResult])
    val condition3 = CheckCondition(status3.timestamp, status3.lifecycle, status3.summary, status3.health,
      status3.correlation, status3.acknowledged, status3.squelched)

    val timestamp4 = today.toDateTime.plusMinutes(4)
    val metrics4 = CheckMetrics(timestamp4, Map("load" -> BigDecimal(4)))
    val status4 = CheckStatus(timestamp4, CheckKnown, Some("healthy4"), CheckHealthy,
      metrics4.metrics, None, None, None, None, squelched = false)
    val notifications4 = CheckNotifications(timestamp4, Vector(NotifyLifecycleChanges(checkRef, timestamp1, CheckJoining, CheckKnown)))
    actor ! UpdateCheckStatus(checkRef, status4, notifications4.notifications, Some(timestamp3))
    expectMsgClass(classOf[UpdateCheckStatusResult])
    val condition4 = CheckCondition(status4.timestamp, status4.lifecycle, status4.summary, status4.health,
      status4.correlation, status4.acknowledged, status4.squelched)

    val timestamp5 = today.toDateTime.plusMinutes(5)
    val metrics5 = CheckMetrics(timestamp5, Map("load" -> BigDecimal(5)))
    val status5 = CheckStatus(timestamp5, CheckKnown, Some("healthy3"), CheckHealthy,
      metrics5.metrics, None, None, None, None, squelched = false)
    val notifications5 = CheckNotifications(timestamp5, Vector(NotifyLifecycleChanges(checkRef, timestamp1, CheckJoining, CheckKnown)))
    actor ! UpdateCheckStatus(checkRef, status5, notifications5.notifications, Some(timestamp4))
    expectMsgClass(classOf[UpdateCheckStatusResult])
    val condition5 = CheckCondition(status5.timestamp, status5.lifecycle, status5.summary, status5.health,
      status5.correlation, status5.acknowledged, status5.squelched)

    "retrieve condition history with no windowing parameters" in {
      actor ! GetConditionHistory(checkRef, None, None, 100, None)
      val getConditionHistoryResult = expectMsgClass(classOf[GetConditionHistoryResult])
      getConditionHistoryResult.page.history shouldEqual Vector(condition1, condition2, condition3, condition4, condition5)
      getConditionHistoryResult.page.exhausted shouldEqual true
    }

    "retrieve condition history with from specified" in {
      actor ! GetConditionHistory(checkRef, Some(timestamp3), None, 100, None)
      val getConditionHistoryResult = expectMsgClass(classOf[GetConditionHistoryResult])
      getConditionHistoryResult.page.history shouldEqual Vector(condition3, condition4, condition5)
      getConditionHistoryResult.page.exhausted shouldEqual true
    }

    "retrieve condition history with to specified" in {
      actor ! GetConditionHistory(checkRef, None, Some(timestamp4), 100, None)
      val getConditionHistoryResult = expectMsgClass(classOf[GetConditionHistoryResult])
      getConditionHistoryResult.page.history shouldEqual Vector(condition1, condition2, condition3)
      getConditionHistoryResult.page.exhausted shouldEqual true
    }

    "retrieve condition history with from and to specified" in {
      actor ! GetConditionHistory(checkRef, Some(timestamp2), Some(timestamp5), 100, None)
      val getConditionHistoryResult = expectMsgClass(classOf[GetConditionHistoryResult])
      getConditionHistoryResult.page.history shouldEqual Vector(condition2, condition3, condition4)
      getConditionHistoryResult.page.exhausted shouldEqual true
    }

    "retrieve notifications history with no windowing parameters" in {
      actor ! GetNotificationsHistory(checkRef, None, None, 100, None)
      val getNotificationHistoryResult = expectMsgClass(classOf[GetNotificationsHistoryResult])
      getNotificationHistoryResult.page.history shouldEqual Vector(notifications1, notifications2, notifications3, notifications4, notifications5)
      getNotificationHistoryResult.page.exhausted shouldEqual true
    }

    "retrieve notifications history with from specified" in {
      actor ! GetNotificationsHistory(checkRef, Some(timestamp3), None, 100, None)
      val getNotificationHistoryResult = expectMsgClass(classOf[GetNotificationsHistoryResult])
      getNotificationHistoryResult.page.history shouldEqual Vector(notifications3, notifications4, notifications5)
      getNotificationHistoryResult.page.exhausted shouldEqual true
    }

    "retrieve notifications history with to specified" in {
      actor ! GetNotificationsHistory(checkRef, None, Some(timestamp4), 100, None)
      val getNotificationHistoryResult = expectMsgClass(classOf[GetNotificationsHistoryResult])
      getNotificationHistoryResult.page.history shouldEqual Vector(notifications1, notifications2, notifications3)
      getNotificationHistoryResult.page.exhausted shouldEqual true
    }

    "retrieve notifications history with from and to specified" in {
      actor ! GetNotificationsHistory(checkRef, Some(timestamp2), Some(timestamp5), 100, None)
      val getNotificationHistoryResult = expectMsgClass(classOf[GetNotificationsHistoryResult])
      getNotificationHistoryResult.page.history shouldEqual Vector(notifications2, notifications3, notifications4)
      getNotificationHistoryResult.page.exhausted shouldEqual true
    }

    "retrieve metrics history with no windowing parameters" in {
      actor ! GetMetricsHistory(checkRef, None, None, 100, None)
      val getMetricHistoryResult = expectMsgClass(classOf[GetMetricsHistoryResult])
      getMetricHistoryResult.page.history shouldEqual Vector(metrics1, metrics2, metrics3, metrics4, metrics5)
      getMetricHistoryResult.page.exhausted shouldEqual true
    }

    "retrieve metrics history with from specified" in {
      actor ! GetMetricsHistory(checkRef, Some(timestamp3), None, 100, None)
      val getMetricHistoryResult = expectMsgClass(classOf[GetMetricsHistoryResult])
      getMetricHistoryResult.page.history shouldEqual Vector(metrics3, metrics4, metrics5)
      getMetricHistoryResult.page.exhausted shouldEqual true
    }

    "retrieve metrics history with to specified" in {
      actor ! GetMetricsHistory(checkRef, None, Some(timestamp4), 100, None)
      val getMetricHistoryResult = expectMsgClass(classOf[GetMetricsHistoryResult])
      getMetricHistoryResult.page.history shouldEqual Vector(metrics1, metrics2, metrics3)
      getMetricHistoryResult.page.exhausted shouldEqual true
    }

    "retrieve metrics history with from and to specified" in {
      actor ! GetMetricsHistory(checkRef, Some(timestamp2), Some(timestamp5), 100, None)
      val getMetricHistoryResult = expectMsgClass(classOf[GetMetricsHistoryResult])
      getMetricHistoryResult.page.history shouldEqual Vector(metrics2, metrics3, metrics4)
      getMetricHistoryResult.page.exhausted shouldEqual true
    }
  }
}
