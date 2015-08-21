/**
 * Copyright 2014 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Mandelbrot.
 *
 * Mandelbrot is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Mandelbrot is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Mandelbrot.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mandelbrot.core.check

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import io.mandelbrot.core.parser.TimeseriesEvaluationParser
import org.joda.time.DateTime
import org.scalatest.ShouldMatchers
import org.scalatest.{WordSpecLike, BeforeAndAfterAll}
import scala.concurrent.duration._
import scala.math.BigDecimal

import io.mandelbrot.core.metrics._
import io.mandelbrot.core.model._
import io.mandelbrot.core.state._
import io.mandelbrot.core.{AkkaConfig, Blackhole}
import io.mandelbrot.core.ConfigConversions._

class TimeseriesCheckSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("TimeseriesCheckSpec", AkkaConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val generation = 1L
  val blackhole = system.actorOf(Blackhole.props())
  val parser = new TimeseriesEvaluationParser()

  "A Check with metrics behavior" should {

    "transition to CheckKnown/CheckHealthy when a healthy MetricsMessage is received" in {
      val checkRef = CheckRef("foo.local:foo.check")
      val source = MetricSource(checkRef.checkId, "value")
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val checkType = "io.mandelbrot.core.check.TimeseriesCheck"
      val factory = CheckBehavior.extensions(checkType).configure(Map("evaluation" -> "when foo.check:value > 10"))
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val check = system.actorOf(Check.props(checkRef, generation, blackhole,  services, metricsBus))
      check ! ChangeCheck(checkType, policy, factory, Set.empty, 0)

      val getStatusHistory = stateService.expectMsgClass(classOf[GetStatusHistory])
      val status = CheckStatus(generation, DateTime.now(), CheckKnown, None, CheckHealthy,
        Map.empty, None, None, None, None, false)
      val page = CheckStatusPage(Vector(status), None, exhausted = true)
      stateService.reply(GetStatusHistoryResult(getStatusHistory, page))

      // check sets its lifecycle to joining
      val updateCheckStatus1 = stateService.expectMsgClass(classOf[UpdateStatus])
      stateService.reply(UpdateStatusResult(updateCheckStatus1))
      updateCheckStatus1.status.lifecycle shouldEqual CheckJoining

      val timestamp = DateTime.now()
      check ! ProcessCheckEvaluation(checkRef, CheckEvaluation(timestamp, None, None, Some(Map(source.metricName -> BigDecimal(5)))))
      val updateCheckStatus2 = stateService.expectMsgClass(classOf[UpdateStatus])
      updateCheckStatus2.status.health shouldEqual CheckHealthy
      updateCheckStatus2.status.correlation shouldEqual None
      updateCheckStatus2.status.acknowledged shouldEqual None
      updateCheckStatus2.status.squelched shouldEqual false
      stateService.reply(UpdateStatusResult(updateCheckStatus2))

      expectMsgClass(classOf[ProcessCheckEvaluationResult])
    }

    "transition to CheckKnown/CheckFailed when a failed MetricsMessage is received" in {
      val checkRef = CheckRef("foo.local:foo.check")
      val source = MetricSource(checkRef.checkId, "value")
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val checkType = "io.mandelbrot.core.check.TimeseriesCheck"
      val factory = CheckBehavior.extensions(checkType).configure(Map("evaluation" -> "when foo.check:value > 10"))
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val check = system.actorOf(Check.props(checkRef, generation, blackhole, services, metricsBus))
      check ! ChangeCheck(checkType, policy, factory, Set.empty, 0)

      val getStatusHistory = stateService.expectMsgClass(classOf[GetStatusHistory])
      val status = CheckStatus(generation, DateTime.now(), CheckKnown, None, CheckHealthy,
        Map.empty, None, None, None, None, false)
      val page = CheckStatusPage(Vector(status), None, exhausted = true)
      stateService.reply(GetStatusHistoryResult(getStatusHistory, page))

      // check sets its lifecycle to joining
      val updateCheckStatus1 = stateService.expectMsgClass(classOf[UpdateStatus])
      stateService.reply(UpdateStatusResult(updateCheckStatus1))
      updateCheckStatus1.status.lifecycle should be(CheckJoining)

      val timestamp = DateTime.now()
      check ! ProcessCheckEvaluation(checkRef, CheckEvaluation(timestamp, None, None, Some(Map(source.metricName -> BigDecimal(15)))))
      val updateCheckStatus2 = stateService.expectMsgClass(classOf[UpdateStatus])
      updateCheckStatus2.status.health shouldEqual CheckFailed
      updateCheckStatus2.status.correlation shouldEqual Some(_: UUID)
      updateCheckStatus2.status.acknowledged shouldEqual None
      updateCheckStatus2.status.squelched shouldEqual false
      stateService.reply(UpdateStatusResult(updateCheckStatus2))

      expectMsgClass(classOf[ProcessCheckEvaluationResult])
    }

    "notify StateService when the joining timeout expires" in {
      val checkRef = CheckRef("foo.local:foo.check")
      val source = MetricSource(checkRef.checkId, "value")
      val policy = CheckPolicy(2.seconds, 1.minute, 1.minute, 1.minute, None)
      val checkType = "io.mandelbrot.core.check.TimeseriesCheck"
      val factory = CheckBehavior.extensions(checkType).configure(Map("evaluation" -> "when foo.check:value > 10"))
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val check = system.actorOf(Check.props(checkRef, generation, blackhole, services, metricsBus))
      check ! ChangeCheck(checkType, policy, factory, Set.empty, 0)

      val getStatusHistory = stateService.expectMsgClass(classOf[GetStatusHistory])
      val status = CheckStatus(generation, DateTime.now(), CheckKnown, None, CheckHealthy,
        Map.empty, None, None, None, None, false)
      val page = CheckStatusPage(Vector(status), None, exhausted = true)
      stateService.reply(GetStatusHistoryResult(getStatusHistory, page))

      // check sets its lifecycle to joining
      val updateCheckStatus1 = stateService.expectMsgClass(classOf[UpdateStatus])
      stateService.reply(UpdateStatusResult(updateCheckStatus1))
      updateCheckStatus1.status.lifecycle shouldEqual CheckJoining

      // expiry timer should fire within 5 seconds
      val updateCheckStatus2 = stateService.expectMsgClass(5.seconds, classOf[UpdateStatus])
      updateCheckStatus2.checkRef shouldEqual checkRef
      updateCheckStatus2.status.health shouldEqual CheckUnknown
      updateCheckStatus2.status.correlation shouldEqual Some(_: UUID)
      updateCheckStatus2.status.acknowledged shouldEqual None
      updateCheckStatus2.status.squelched shouldEqual false
    }

    "notify StateService when the check timeout expires" in {
      val checkRef = CheckRef("foo.local:foo.check")
      val source = MetricSource(checkRef.checkId, "value")
      val evaluation = parser.parseTimeseriesEvaluation("when foo.check:value > 10")
      val policy = CheckPolicy(1.minute, 2.seconds, 1.minute, 1.minute, None)
      val checkType = "io.mandelbrot.core.check.TimeseriesCheck"
      val factory = CheckBehavior.extensions(checkType).configure(Map("evaluation" -> "when foo.check:value > 10"))
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val check = system.actorOf(Check.props(checkRef, generation, blackhole, services, metricsBus))
      check ! ChangeCheck(checkType, policy, factory, Set.empty, 0)

      val getStatusHistory = stateService.expectMsgClass(classOf[GetStatusHistory])
      val status = CheckStatus(generation, DateTime.now(), CheckKnown, None, CheckHealthy,
        Map.empty, None, None, None, None, false)
      val page = CheckStatusPage(Vector(status), None, exhausted = true)
      stateService.reply(GetStatusHistoryResult(getStatusHistory, page))

      // check sets its lifecycle to joining
      val updateCheckStatus1 = stateService.expectMsgClass(classOf[UpdateStatus])
      stateService.reply(UpdateStatusResult(updateCheckStatus1))
      updateCheckStatus1.status.lifecycle shouldEqual CheckJoining

      val timestamp = DateTime.now()
      check ! ProcessCheckEvaluation(checkRef, CheckEvaluation(timestamp, None, None, Some(Map(source.metricName -> BigDecimal(5)))))
      val updateCheckStatus2 = stateService.expectMsgClass(classOf[UpdateStatus])
      stateService.reply(UpdateStatusResult(updateCheckStatus2))
      updateCheckStatus2.status.lifecycle shouldEqual CheckKnown
      updateCheckStatus2.status.health shouldEqual CheckHealthy
      expectMsgClass(classOf[ProcessCheckEvaluationResult])

      // expiry timer should fire within 5 seconds
      val updateCheckStatus3 = stateService.expectMsgClass(5.seconds, classOf[UpdateStatus])
      updateCheckStatus3.checkRef shouldEqual checkRef
      updateCheckStatus3.status.lifecycle shouldEqual CheckKnown
      updateCheckStatus3.status.health shouldEqual CheckUnknown
      updateCheckStatus3.status.summary shouldEqual None
      updateCheckStatus3.status.correlation shouldEqual Some(_: UUID)
      updateCheckStatus3.status.acknowledged shouldEqual None
      updateCheckStatus3.status.squelched shouldEqual false
    }

    "notify NotificationService when the alert timeout expires" in {
      val checkRef = CheckRef("foo.local:foo.check")
      val source = MetricSource(checkRef.checkId, "value")
      val policy = CheckPolicy(1.minute, 1.minute, 2.seconds, 1.minute, None)
      val checkType = "io.mandelbrot.core.check.TimeseriesCheck"
      val factory = CheckBehavior.extensions(checkType).configure(Map("evaluation" -> "when foo.check:value > 10"))
      val notificationService = new TestProbe(_system)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref), notificationService = Some(notificationService.ref)))
      val metricsBus = new MetricsBus()

      val check = system.actorOf(Check.props(checkRef, generation, blackhole, services, metricsBus))
      check ! ChangeCheck(checkType, policy, factory, Set.empty, 0)

      val getStatusHistory = stateService.expectMsgClass(classOf[GetStatusHistory])
      val status = CheckStatus(generation, DateTime.now(), CheckKnown, None, CheckHealthy,
        Map.empty, None, None, None, None, false)
      val page = CheckStatusPage(Vector(status), None, exhausted = true)
      stateService.reply(GetStatusHistoryResult(getStatusHistory, page))

      // check sets its lifecycle to joining
      val updateCheckStatus1 = stateService.expectMsgClass(classOf[UpdateStatus])
      stateService.reply(UpdateStatusResult(updateCheckStatus1))
      updateCheckStatus1.status.lifecycle shouldEqual CheckJoining

      val timestamp = DateTime.now()
      check ! ProcessCheckEvaluation(checkRef, CheckEvaluation(timestamp, None, None, Some(Map(source.metricName -> BigDecimal(15)))))
      val updateCheckStatus2 = stateService.expectMsgClass(classOf[UpdateStatus])
      stateService.reply(UpdateStatusResult(updateCheckStatus2))
      notificationService.expectMsgClass(classOf[NotifyLifecycleChanges])
      notificationService.expectMsgClass(classOf[NotifyHealthChanges])
      expectMsgClass(classOf[ProcessCheckEvaluationResult])

      // expiry timer should fire within 5 seconds
      val updateCheckStatus3 = stateService.expectMsgClass(5.seconds, classOf[UpdateStatus])
      stateService.reply(UpdateStatusResult(updateCheckStatus3))
      val notification = notificationService.expectMsgClass(classOf[NotifyHealthAlerts])
      notification.checkRef shouldEqual checkRef
      notification.health shouldEqual CheckFailed
      notification.correlation shouldEqual updateCheckStatus2.status.correlation
    }

  }
}
