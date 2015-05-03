package io.mandelbrot.core.state

import akka.actor.{PoisonPill, ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.{WordSpecLike, ShouldMatchers, BeforeAndAfterAll}
import org.scalatest.LoneElement._
import org.scalatest.Inside._

import io.mandelbrot.core.model._
import io.mandelbrot.core.util.Timestamp
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

  val timestamp1 = today.toDateTime.plusMinutes(1)
  val metrics1 = CheckMetrics(timestamp1, Map("load" -> BigDecimal(1)))
  val status1 = CheckStatus(timestamp1, CheckKnown, Some("healthy1"), CheckHealthy,
    metrics1.metrics, None, None, None, None, squelched = false)
  val notifications1 = CheckNotifications(timestamp1, Vector(NotifyLifecycleChanges(checkRef, timestamp1, CheckJoining, CheckKnown)))
  val condition1 = CheckCondition(status1.timestamp, status1.lifecycle, status1.summary, status1.health,
    status1.correlation, status1.acknowledged, status1.squelched)

  val timestamp2 = today.toDateTime.plusMinutes(2)
  val metrics2 = CheckMetrics(timestamp2, Map("load" -> BigDecimal(2)))
  val status2 = CheckStatus(timestamp2, CheckKnown, Some("healthy2"), CheckHealthy,
    metrics2.metrics, None, None, None, None, squelched = false)
  val notifications2 = CheckNotifications(timestamp2, Vector(NotifyLifecycleChanges(checkRef, timestamp2, CheckJoining, CheckKnown)))
  val condition2 = CheckCondition(status2.timestamp, status2.lifecycle, status2.summary, status2.health,
    status2.correlation, status2.acknowledged, status2.squelched)

  val timestamp3 = today.toDateTime.plusMinutes(3)
  val metrics3 = CheckMetrics(timestamp3, Map("load" -> BigDecimal(3)))
  val status3 = CheckStatus(timestamp3, CheckKnown, Some("healthy3"), CheckHealthy,
    metrics3.metrics, None, None, None, None, squelched = false)
  val notifications3 = CheckNotifications(timestamp3, Vector(NotifyLifecycleChanges(checkRef, timestamp3, CheckJoining, CheckKnown)))
  val condition3 = CheckCondition(status3.timestamp, status3.lifecycle, status3.summary, status3.health,
    status3.correlation, status3.acknowledged, status3.squelched)

  val timestamp4 = today.toDateTime.plusMinutes(4)
  val metrics4 = CheckMetrics(timestamp4, Map("load" -> BigDecimal(4)))
  val status4 = CheckStatus(timestamp4, CheckKnown, Some("healthy4"), CheckHealthy,
    metrics4.metrics, None, None, None, None, squelched = false)
  val notifications4 = CheckNotifications(timestamp4, Vector(NotifyLifecycleChanges(checkRef, timestamp4, CheckJoining, CheckKnown)))
  val condition4 = CheckCondition(status4.timestamp, status4.lifecycle, status4.summary, status4.health,
    status4.correlation, status4.acknowledged, status4.squelched)

  val timestamp5 = today.toDateTime.plusMinutes(5)
  val metrics5 = CheckMetrics(timestamp5, Map("load" -> BigDecimal(5)))
  val status5 = CheckStatus(timestamp5, CheckKnown, Some("healthy5"), CheckHealthy,
    metrics5.metrics, None, None, None, None, squelched = false)
  val notifications5 = CheckNotifications(timestamp5, Vector(NotifyLifecycleChanges(checkRef, timestamp5, CheckJoining, CheckKnown)))
  val condition5 = CheckCondition(status5.timestamp, status5.lifecycle, status5.summary, status5.health,
    status5.correlation, status5.acknowledged, status5.squelched)

  def withTestData(testCode: (ActorRef) => Any): Unit = {
    withStateService { stateService =>

      stateService ! UpdateCheckStatus(checkRef, status1, notifications1.notifications, lastTimestamp = None)
      val updateCheckStatusResult1 = expectMsgClass(classOf[UpdateCheckStatusResult])

      stateService ! UpdateCheckStatus(checkRef, status2, notifications2.notifications, Some(timestamp1))
      val updateCheckStatusResult2 = expectMsgClass(classOf[UpdateCheckStatusResult])

      stateService ! UpdateCheckStatus(checkRef, status3, notifications3.notifications, Some(timestamp2))
      val updateCheckStatusResult3 = expectMsgClass(classOf[UpdateCheckStatusResult])

      stateService ! UpdateCheckStatus(checkRef, status4, notifications4.notifications, Some(timestamp3))
      val updateCheckStatusResult4 = expectMsgClass(classOf[UpdateCheckStatusResult])

      stateService ! UpdateCheckStatus(checkRef, status5, notifications5.notifications, Some(timestamp4))
      val updateCheckStatusResult5 = expectMsgClass(classOf[UpdateCheckStatusResult])

      testCode(stateService)
    }
  }

  "A StateManager" when {

    "servicing a InitializeCheckStatus request" should {

      "return None if the check doesn't exist" in withStateService { stateService =>
        stateService ! InitializeCheckStatus(checkRef, timestamp1)
        val initializeCheckStatusResult = expectMsgClass(classOf[InitializeCheckStatusResult])
        initializeCheckStatusResult.status shouldEqual None
      }

      "return Some(status) if the check exists" in withStateService { stateService =>
        stateService ! InitializeCheckStatus(checkRef, timestamp1)
        val initializeCheckStatusResult1 = expectMsgClass(classOf[InitializeCheckStatusResult])
        initializeCheckStatusResult1.status shouldEqual None

        stateService ! UpdateCheckStatus(checkRef, status1, notifications1.notifications, lastTimestamp = None)
        val updateCheckStatusResult = expectMsgClass(classOf[UpdateCheckStatusResult])

        stateService ! InitializeCheckStatus(checkRef, timestamp1)
        val initializeCheckStatusResult2 = expectMsgClass(classOf[InitializeCheckStatusResult])
        inside(initializeCheckStatusResult2.status) {
          case Some(initialStatus) =>
            initialStatus shouldEqual status1
        }
      }
    }

    "A StateManager servicing an UpdateCheckStatus request" should {

      "update check status if the check doesn't exist" in withStateService { stateService =>
        stateService ! UpdateCheckStatus(checkRef, status1, notifications1.notifications, lastTimestamp = None)
        val updateCheckStatusResult = expectMsgClass(classOf[UpdateCheckStatusResult])
      }

      "update check status if the check exists" in withStateService { stateService =>
        stateService ! UpdateCheckStatus(checkRef, status1, notifications1.notifications, lastTimestamp = None)
        val updateCheckStatusResult1 = expectMsgClass(classOf[UpdateCheckStatusResult])
        stateService ! UpdateCheckStatus(checkRef, status2, notifications2.notifications, Some(status1.timestamp))
        val updateCheckStatusResult2 = expectMsgClass(classOf[UpdateCheckStatusResult])
      }
    }

    "A StateManager servicing a GetConditionHistory request" should {

      "return ResourceNotFound if check doesn't exist" in withStateService { stateService =>
        stateService ! GetConditionHistory(checkRef, None, None, 10, None)
        val getConditionHistoryResult = expectMsgClass(classOf[StateServiceOperationFailed])
        getConditionHistoryResult.failure shouldEqual ApiException(ResourceNotFound)
      }

      "return the last condition as the only element in a page if timeseries parameters are not specified" in withTestData { stateService =>
        stateService ! GetConditionHistory(checkRef, None, None, 10, None)
        val getConditionHistoryResult = expectMsgClass(classOf[GetConditionHistoryResult])
        getConditionHistoryResult.page.last shouldEqual None
        getConditionHistoryResult.page.exhausted shouldEqual true
        val condition = getConditionHistoryResult.page.history.loneElement
        condition shouldEqual condition5
      }

      "return a page of condition history newer than 'from' when 'from' is specified" in withTestData { stateService =>
        stateService ! GetConditionHistory(checkRef, Some(timestamp3), None, 100, None)
        val getConditionHistoryResult = expectMsgClass(classOf[GetConditionHistoryResult])
        getConditionHistoryResult.page.history shouldEqual Vector(condition4, condition5)
        getConditionHistoryResult.page.last shouldEqual None
        getConditionHistoryResult.page.exhausted shouldEqual true
      }

      "return a page of condition history older than 'to' when 'to' is specified" in withTestData { stateService =>
        stateService ! GetConditionHistory(checkRef, None, Some(timestamp4), 100, None)
        val getConditionHistoryResult = expectMsgClass(classOf[GetConditionHistoryResult])
        getConditionHistoryResult.page.history shouldEqual Vector(condition1, condition2, condition3)
        getConditionHistoryResult.page.last shouldEqual None
        getConditionHistoryResult.page.exhausted shouldEqual true
      }

      "return a page of condition history between 'from' and 'to' when 'from' and 'to' are specified" in withTestData { stateService =>
        stateService ! GetConditionHistory(checkRef, Some(timestamp2), Some(timestamp5), 100, None)
        val getConditionHistoryResult = expectMsgClass(classOf[GetConditionHistoryResult])
        getConditionHistoryResult.page.history shouldEqual Vector(condition3, condition4)
        getConditionHistoryResult.page.last shouldEqual None
        getConditionHistoryResult.page.exhausted shouldEqual true
      }

    }
  }
}

