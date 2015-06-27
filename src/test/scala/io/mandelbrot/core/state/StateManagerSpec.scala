package io.mandelbrot.core.state

import akka.actor.{PoisonPill, ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.{WordSpecLike, ShouldMatchers, BeforeAndAfterAll}
import org.scalatest.LoneElement._
import org.scalatest.Inside._

import io.mandelbrot.core.model._
import io.mandelbrot.core._
import io.mandelbrot.core.ConfigConversions._

class StateManagerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("StateManagerSpec", AkkaConfig ++ MandelbrotConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val settings = ServerConfig(system).settings

  def withStateService(testCode: (ActorRef) => Any) {
    val stateService = system.actorOf(StateManager.props(settings.state))
    testCode(stateService)
    stateService ! PoisonPill
  }

  val today = new DateTime(DateTimeZone.UTC).toDateMidnight
  val checkRef = CheckRef("test.state.manager:check")
  val generation = 1L

  val timestamp1 = today.toDateTime.plusMinutes(1)
  val metrics1 = CheckMetrics(generation, timestamp1, Map("load" -> BigDecimal(1)))
  val status1 = CheckStatus(generation, timestamp1, CheckKnown, Some("healthy1"), CheckHealthy,
    metrics1.metrics, None, None, None, None, squelched = false)
  val notifications1 = CheckNotifications(generation, timestamp1,
    Vector(NotifyLifecycleChanges(checkRef, timestamp1, CheckJoining, CheckKnown)))
  val condition1 = CheckCondition(generation, status1.timestamp, status1.lifecycle, status1.summary, status1.health,
    status1.correlation, status1.acknowledged, status1.squelched)

  val timestamp2 = today.toDateTime.plusMinutes(2)
  val metrics2 = CheckMetrics(generation, timestamp2, Map("load" -> BigDecimal(2)))
  val status2 = CheckStatus(generation, timestamp2, CheckKnown, Some("healthy2"), CheckHealthy,
    metrics2.metrics, None, None, None, None, squelched = false)
  val notifications2 = CheckNotifications(generation, timestamp2,
    Vector(NotifyLifecycleChanges(checkRef, timestamp2, CheckJoining, CheckKnown)))
  val condition2 = CheckCondition(generation, status2.timestamp, status2.lifecycle, status2.summary, status2.health,
    status2.correlation, status2.acknowledged, status2.squelched)

  val timestamp3 = today.toDateTime.plusMinutes(3)
  val metrics3 = CheckMetrics(generation, timestamp3, Map("load" -> BigDecimal(3)))
  val status3 = CheckStatus(generation, timestamp3, CheckKnown, Some("healthy3"), CheckHealthy,
    metrics3.metrics, None, None, None, None, squelched = false)
  val notifications3 = CheckNotifications(generation, timestamp3,
    Vector(NotifyLifecycleChanges(checkRef, timestamp3, CheckJoining, CheckKnown)))
  val condition3 = CheckCondition(generation, status3.timestamp, status3.lifecycle, status3.summary, status3.health,
    status3.correlation, status3.acknowledged, status3.squelched)

  val timestamp4 = today.toDateTime.plusMinutes(4)
  val metrics4 = CheckMetrics(generation, timestamp4, Map("load" -> BigDecimal(4)))
  val status4 = CheckStatus(generation, timestamp4, CheckKnown, Some("healthy4"), CheckHealthy,
    metrics4.metrics, None, None, None, None, squelched = false)
  val notifications4 = CheckNotifications(generation, timestamp4,
    Vector(NotifyLifecycleChanges(checkRef, timestamp4, CheckJoining, CheckKnown)))
  val condition4 = CheckCondition(generation, status4.timestamp, status4.lifecycle, status4.summary, status4.health,
    status4.correlation, status4.acknowledged, status4.squelched)

  val timestamp5 = today.toDateTime.plusMinutes(5)
  val metrics5 = CheckMetrics(generation, timestamp5, Map("load" -> BigDecimal(5)))
  val status5 = CheckStatus(generation, timestamp5, CheckKnown, Some("healthy5"), CheckHealthy,
    metrics5.metrics, None, None, None, None, squelched = false)
  val notifications5 = CheckNotifications(generation, timestamp5,
    Vector(NotifyLifecycleChanges(checkRef, timestamp5, CheckJoining, CheckKnown)))
  val condition5 = CheckCondition(generation, status5.timestamp, status5.lifecycle, status5.summary, status5.health,
    status5.correlation, status5.acknowledged, status5.squelched)

  def withTestData(testCode: (ActorRef) => Any): Unit = {
    withStateService { stateService =>

      stateService ! UpdateStatus(checkRef, status1, notifications1.notifications)
      val updateCheckStatusResult1 = expectMsgClass(classOf[UpdateStatusResult])

      stateService ! UpdateStatus(checkRef, status2, notifications2.notifications)
      val updateCheckStatusResult2 = expectMsgClass(classOf[UpdateStatusResult])

      stateService ! UpdateStatus(checkRef, status3, notifications3.notifications)
      val updateCheckStatusResult3 = expectMsgClass(classOf[UpdateStatusResult])

      stateService ! UpdateStatus(checkRef, status4, notifications4.notifications)
      val updateCheckStatusResult4 = expectMsgClass(classOf[UpdateStatusResult])

      stateService ! UpdateStatus(checkRef, status5, notifications5.notifications)
      val updateCheckStatusResult5 = expectMsgClass(classOf[UpdateStatusResult])

      testCode(stateService)
    }
  }

  "A StateManager" when {

    "servicing a GetStatus request" should {

      "return None if the check doesn't exist" in withStateService { stateService =>
        stateService ! GetStatus(checkRef, generation)
        val initializeCheckStatusResult = expectMsgClass(classOf[GetStatusResult])
        initializeCheckStatusResult.status shouldEqual None
      }

      "return Some(status) if the check exists" in withStateService { stateService =>
        stateService ! GetStatus(checkRef, generation)
        val initializeCheckStatusResult1 = expectMsgClass(classOf[GetStatusResult])
        initializeCheckStatusResult1.status shouldEqual None

        stateService ! UpdateStatus(checkRef, status1, notifications1.notifications)
        val updateCheckStatusResult = expectMsgClass(classOf[UpdateStatusResult])

        stateService ! GetStatus(checkRef, generation)
        val initializeCheckStatusResult2 = expectMsgClass(classOf[GetStatusResult])
        inside(initializeCheckStatusResult2.status) {
          case Some(initialStatus) =>
            initialStatus shouldEqual status1
        }
      }
    }

    "servicing an UpdateStatus request" should {

      "update check status if the check doesn't exist" in withStateService { stateService =>
        stateService ! UpdateStatus(checkRef, status1, notifications1.notifications)
        val updateCheckStatusResult = expectMsgClass(classOf[UpdateStatusResult])
      }

      "update check status if the check exists" in withStateService { stateService =>
        stateService ! UpdateStatus(checkRef, status1, notifications1.notifications)
        val updateCheckStatusResult1 = expectMsgClass(classOf[UpdateStatusResult])
        stateService ! UpdateStatus(checkRef, status2, notifications2.notifications)
        val updateCheckStatusResult2 = expectMsgClass(classOf[UpdateStatusResult])
      }
    }

    "servicing a DeleteStatus request without 'until' parameter specified" should {

      "delete check status if the check doesn't exist" in withStateService { stateService =>
        stateService ! DeleteStatus(checkRef, generation)
        val deleteCheckStatusResult = expectMsgClass(classOf[DeleteStatusResult])
      }

      "delete check status if the check exists" in withStateService { stateService =>
        stateService ! UpdateStatus(checkRef, status1, notifications1.notifications)
        val updateCheckStatusResult1 = expectMsgClass(classOf[UpdateStatusResult])
        stateService ! UpdateStatus(checkRef, status2, notifications2.notifications)
        val updateCheckStatusResult2 = expectMsgClass(classOf[UpdateStatusResult])
        stateService ! UpdateStatus(checkRef, status3, notifications3.notifications)
        val updateCheckStatusResult3 = expectMsgClass(classOf[UpdateStatusResult])

        stateService ! DeleteStatus(checkRef, generation)
        val deleteCheckStatusResult = expectMsgClass(classOf[DeleteStatusResult])

        stateService ! GetStatus(checkRef, generation)
        val initializeCheckStatusResult = expectMsgClass(classOf[GetStatusResult])
        initializeCheckStatusResult.status shouldEqual None
      }

    }

    "servicing a DeleteStatus request with 'until' parameter specified" should {

      "trim check history if the check doesn't exist" in withStateService { stateService =>
        stateService ! DeleteStatus(checkRef, generation)
        val trimCheckHistoryResult = expectMsgClass(classOf[DeleteStatusResult])
      }

      "trim check history to the specified point if the check exists" in withStateService { stateService =>
        stateService ! UpdateStatus(checkRef, status1, notifications1.notifications)
        val updateCheckStatusResult1 = expectMsgClass(classOf[UpdateStatusResult])
        stateService ! UpdateStatus(checkRef, status2, notifications2.notifications)
        val updateCheckStatusResult2 = expectMsgClass(classOf[UpdateStatusResult])
        stateService ! UpdateStatus(checkRef, status3, notifications3.notifications)
        val updateCheckStatusResult3 = expectMsgClass(classOf[UpdateStatusResult])

        stateService ! DeleteStatus(checkRef, generation)
        val trimCheckHistoryResult = expectMsgClass(classOf[DeleteStatusResult])
      }
    }

    "servicing a GetConditionHistory request" should {

      "return ResourceNotFound if check doesn't exist" in withStateService { stateService =>
        stateService ! GetConditionHistory(checkRef, generation, None, None, 10)
        val getConditionHistoryResult = expectMsgClass(classOf[StateServiceOperationFailed])
        getConditionHistoryResult.failure shouldEqual ApiException(ResourceNotFound)
      }

      "return the last condition as the only element in a page if timeseries parameters are not specified" in withTestData { stateService =>
        stateService ! GetConditionHistory(checkRef, generation, None, None, 10)
        val getConditionHistoryResult = expectMsgClass(classOf[GetConditionHistoryResult])
        getConditionHistoryResult.page.last shouldEqual None
        getConditionHistoryResult.page.exhausted shouldEqual true
        val condition = getConditionHistoryResult.page.history.loneElement
        condition shouldEqual condition5
      }

      "return a page of condition history newer than 'from' when 'from' is specified" in withTestData { stateService =>
        stateService ! GetConditionHistory(checkRef, generation, Some(timestamp3), None, 100)
        val getConditionHistoryResult = expectMsgClass(classOf[GetConditionHistoryResult])
        getConditionHistoryResult.page.history shouldEqual Vector(condition4, condition5)
        getConditionHistoryResult.page.last shouldEqual None
        getConditionHistoryResult.page.exhausted shouldEqual true
      }

      "return a page of condition history older than 'to' when 'to' is specified" in withTestData { stateService =>
        stateService ! GetConditionHistory(checkRef, generation, None, Some(timestamp4), 100)
        val getConditionHistoryResult = expectMsgClass(classOf[GetConditionHistoryResult])
        getConditionHistoryResult.page.history shouldEqual Vector(condition1, condition2, condition3, condition4)
        getConditionHistoryResult.page.last shouldEqual None
        getConditionHistoryResult.page.exhausted shouldEqual true
      }

      "return a page of condition history between 'from' and 'to' when 'from' and 'to' are specified" in withTestData { stateService =>
        stateService ! GetConditionHistory(checkRef, generation, Some(timestamp2), Some(timestamp4), 100)
        val getConditionHistoryResult = expectMsgClass(classOf[GetConditionHistoryResult])
        getConditionHistoryResult.page.history shouldEqual Vector(condition3, condition4)
        getConditionHistoryResult.page.last shouldEqual None
        getConditionHistoryResult.page.exhausted shouldEqual true
      }
    }

    "servicing a GetNotificationsHistory request" should {

      "return ResourceNotFound if check doesn't exist" in withStateService { stateService =>
        stateService ! GetNotificationsHistory(checkRef, generation, None, None, 10)
        val getNotificationHistoryResult = expectMsgClass(classOf[StateServiceOperationFailed])
        getNotificationHistoryResult.failure shouldEqual ApiException(ResourceNotFound)
      }

      "return the last notifications as the only element in a page if timeseries parameters are not specified" in withTestData { stateService =>
        stateService ! GetNotificationsHistory(checkRef, generation, None, None, 10)
        val getNotificationHistoryResult = expectMsgClass(classOf[GetNotificationsHistoryResult])
        getNotificationHistoryResult.page.last shouldEqual None
        getNotificationHistoryResult.page.exhausted shouldEqual true
        val notifications = getNotificationHistoryResult.page.history.loneElement
        notifications shouldEqual notifications5
      }

      "return a page of notifications history newer than 'from' when 'from' is specified" in withTestData { stateService =>
        stateService ! GetNotificationsHistory(checkRef, generation, Some(timestamp3), None, 100)
        val getNotificationHistoryResult = expectMsgClass(classOf[GetNotificationsHistoryResult])
        getNotificationHistoryResult.page.history shouldEqual Vector(notifications4, notifications5)
        getNotificationHistoryResult.page.last shouldEqual None
        getNotificationHistoryResult.page.exhausted shouldEqual true
      }

      "return a page of notifications history older than 'to' when 'to' is specified" in withTestData { stateService =>
        stateService ! GetNotificationsHistory(checkRef, generation, None, Some(timestamp4), 100)
        val getNotificationHistoryResult = expectMsgClass(classOf[GetNotificationsHistoryResult])
        getNotificationHistoryResult.page.history shouldEqual Vector(notifications1, notifications2, notifications3, notifications4)
        getNotificationHistoryResult.page.last shouldEqual None
        getNotificationHistoryResult.page.exhausted shouldEqual true
      }

      "return a page of notifications history between 'from' and 'to' when 'from' and 'to' are specified" in withTestData { stateService =>
        stateService ! GetNotificationsHistory(checkRef, generation, Some(timestamp2), Some(timestamp4), 100)
        val getNotificationHistoryResult = expectMsgClass(classOf[GetNotificationsHistoryResult])
        getNotificationHistoryResult.page.history shouldEqual Vector(notifications3, notifications4)
        getNotificationHistoryResult.page.last shouldEqual None
        getNotificationHistoryResult.page.exhausted shouldEqual true
      }
    }

    "servicing a GetMetricssHistory request" should {

      "return ResourceNotFound if check doesn't exist" in withStateService { stateService =>
        stateService ! GetMetricsHistory(checkRef, generation, None, None, 10)
        val getMetricsHistoryResult = expectMsgClass(classOf[StateServiceOperationFailed])
        getMetricsHistoryResult.failure shouldEqual ApiException(ResourceNotFound)
      }

      "return the last metrics as the only element in a page if timeseries parameters are not specified" in withTestData { stateService =>
        stateService ! GetMetricsHistory(checkRef, generation, None, None, 10)
        val getMetricsHistoryResult = expectMsgClass(classOf[GetMetricsHistoryResult])
        getMetricsHistoryResult.page.last shouldEqual None
        getMetricsHistoryResult.page.exhausted shouldEqual true
        val metrics = getMetricsHistoryResult.page.history.loneElement
        metrics shouldEqual metrics5
      }

      "return a page of metrics history newer than 'from' when 'from' is specified" in withTestData { stateService =>
        stateService ! GetMetricsHistory(checkRef, generation, Some(timestamp3), None, 100)
        val getMetricsHistoryResult = expectMsgClass(classOf[GetMetricsHistoryResult])
        getMetricsHistoryResult.page.history shouldEqual Vector(metrics4, metrics5)
        getMetricsHistoryResult.page.last shouldEqual None
        getMetricsHistoryResult.page.exhausted shouldEqual true
      }

      "return a page of metrics history older than 'to' when 'to' is specified" in withTestData { stateService =>
        stateService ! GetMetricsHistory(checkRef, generation, None, Some(timestamp4), 100)
        val getMetricsHistoryResult = expectMsgClass(classOf[GetMetricsHistoryResult])
        getMetricsHistoryResult.page.history shouldEqual Vector(metrics1, metrics2, metrics3, metrics4)
        getMetricsHistoryResult.page.last shouldEqual None
        getMetricsHistoryResult.page.exhausted shouldEqual true
      }

      "return a page of metrics history between 'from' and 'to' when 'from' and 'to' are specified" in withTestData { stateService =>
        stateService ! GetMetricsHistory(checkRef, generation, Some(timestamp2), Some(timestamp4), 100)
        val getMetricsHistoryResult = expectMsgClass(classOf[GetMetricsHistoryResult])
        getMetricsHistoryResult.page.history shouldEqual Vector(metrics3, metrics4)
        getMetricsHistoryResult.page.last shouldEqual None
        getMetricsHistoryResult.page.exhausted shouldEqual true
      }
    }
  }
}

