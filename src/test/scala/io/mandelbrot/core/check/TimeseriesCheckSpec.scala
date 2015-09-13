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
import io.mandelbrot.core.agent.{ProcessProbeObservationResult, ProcessProbeObservation, ObservationBus}
import io.mandelbrot.core.parser.TimeseriesEvaluationParser
import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.ShouldMatchers
import org.scalatest.{WordSpecLike, BeforeAndAfterAll}
import scala.concurrent.duration._
import scala.math.BigDecimal

import io.mandelbrot.core.metrics._
import io.mandelbrot.core.model._
import io.mandelbrot.core.state._
import io.mandelbrot.core.{ResourceNotFound, ApiException, AkkaConfig, Blackhole}
import io.mandelbrot.core.ConfigConversions._

class TimeseriesCheckSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("TimeseriesCheckSpec", AkkaConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val generation = 1L
  val blackhole = system.actorOf(Blackhole.props())
  val parser = TimeseriesEvaluationParser.parser

  "A Check with metrics behavior" should {

    "transition to CheckKnown/CheckHealthy when a healthy MetricsMessage is received" in {
      val checkRef = CheckRef("foo.local:foo.check")
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val checkType = "io.mandelbrot.core.check.TimeseriesCheck"
      val factory = CheckBehavior.extensions(checkType).configure(Map("evaluation" -> "probe:foo.check:value > 10"))
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new ObservationBus()

      // create check actor
      val check = system.actorOf(Check.props(checkRef, generation, blackhole,  services, metricsBus))

      // initialize check
      check ! ChangeCheck(checkType, policy, factory, Set.empty, 0)

      val getStatus = stateService.expectMsgClass(classOf[GetStatus])
      val status = CheckStatus(generation, DateTime.now(), CheckKnown, None, CheckHealthy,
        Map.empty, None, None, None, None, false)
      stateService.reply(GetStatusResult(getStatus, Some(status)))

      val getObservationHistory = stateService.expectMsgClass(classOf[GetObservationHistory])
      stateService.reply(StateServiceOperationFailed(getObservationHistory, ApiException(ResourceNotFound)))

      // check sets its lifecycle to known
      val updateStatus1 = stateService.expectMsgClass(classOf[UpdateStatus])
      stateService.reply(UpdateStatusResult(updateStatus1))
      updateStatus1.status.lifecycle shouldEqual CheckKnown

      val timestamp = DateTime.now(DateTimeZone.UTC)
      check ! ProcessObservation(ProbeId("foo.check"), ScalarMapObservation(timestamp, Map("value" -> BigDecimal(5))))
      val updateStatus2 = stateService.expectMsgClass(classOf[UpdateStatus])
      updateStatus2.status.health shouldEqual CheckHealthy
      updateStatus2.status.correlation shouldEqual None
      updateStatus2.status.acknowledged shouldEqual None
      updateStatus2.status.squelched shouldEqual false
      stateService.reply(UpdateStatusResult(updateStatus2))
    }

    "transition to CheckKnown/CheckFailed when a failed MetricsMessage is received" in {
      val checkRef = CheckRef("foo.local:foo.check")
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val checkType = "io.mandelbrot.core.check.TimeseriesCheck"
      val factory = CheckBehavior.extensions(checkType).configure(Map("evaluation" -> "probe:foo.check:value > 10"))
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new ObservationBus()

      // create check actor
      val check = system.actorOf(Check.props(checkRef, generation, blackhole, services, metricsBus))

      // initialize check
      check ! ChangeCheck(checkType, policy, factory, Set.empty, 0)

      val getStatus = stateService.expectMsgClass(classOf[GetStatus])
      val status = CheckStatus(generation, DateTime.now(), CheckKnown, None, CheckHealthy,
        Map.empty, None, None, None, None, false)
      stateService.reply(GetStatusResult(getStatus, Some(status)))

      val getObservationHistory = stateService.expectMsgClass(classOf[GetObservationHistory])
      stateService.reply(StateServiceOperationFailed(getObservationHistory, ApiException(ResourceNotFound)))

      // check sets its lifecycle to known
      val updateStatus1 = stateService.expectMsgClass(classOf[UpdateStatus])
      stateService.reply(UpdateStatusResult(updateStatus1))
      updateStatus1.status.lifecycle should be(CheckKnown)

      val timestamp = DateTime.now()
      check ! ProcessObservation(ProbeId("foo.check"), ScalarMapObservation(timestamp, Map("value" -> BigDecimal(15))))
      val updateStatus2 = stateService.expectMsgClass(classOf[UpdateStatus])
      updateStatus2.status.health shouldEqual CheckFailed
      updateStatus2.status.correlation shouldEqual Some(_: UUID)
      updateStatus2.status.acknowledged shouldEqual None
      updateStatus2.status.squelched shouldEqual false
      stateService.reply(UpdateStatusResult(updateStatus2))
    }

    "notify StateService when the joining timeout expires" in {
      val checkRef = CheckRef("foo.local:foo.check")
      val policy = CheckPolicy(2.seconds, 1.minute, 1.minute, 1.minute, None)
      val checkType = "io.mandelbrot.core.check.TimeseriesCheck"
      val factory = CheckBehavior.extensions(checkType).configure(Map("evaluation" -> "probe:foo.check:value > 10"))
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new ObservationBus()

      // create check actor
      val check = system.actorOf(Check.props(checkRef, generation, blackhole, services, metricsBus))

      // initialize check
      check ! ChangeCheck(checkType, policy, factory, Set.empty, 0)

      val getStatus = stateService.expectMsgClass(classOf[GetStatus])
      val status = CheckStatus(generation, DateTime.now(), CheckKnown, None, CheckHealthy,
        Map.empty, None, None, None, None, false)
      stateService.reply(GetStatusResult(getStatus, Some(status)))

      val getObservationHistory = stateService.expectMsgClass(classOf[GetObservationHistory])
      stateService.reply(StateServiceOperationFailed(getObservationHistory, ApiException(ResourceNotFound)))

      // check sets its lifecycle to known
      val updateStatus1 = stateService.expectMsgClass(classOf[UpdateStatus])
      stateService.reply(UpdateStatusResult(updateStatus1))
      updateStatus1.status.lifecycle shouldEqual CheckKnown

      // expiry timer should fire within 5 seconds
      val updateStatus2 = stateService.expectMsgClass(5.seconds, classOf[UpdateStatus])
      updateStatus2.checkRef shouldEqual checkRef
      updateStatus2.status.health shouldEqual CheckUnknown
      updateStatus2.status.correlation shouldEqual Some(_: UUID)
      updateStatus2.status.acknowledged shouldEqual None
      updateStatus2.status.squelched shouldEqual false
    }

    "notify StateService when the check timeout expires" in {
      val checkRef = CheckRef("foo.local:foo.check")
      val evaluation = parser.parseTimeseriesEvaluation("probe:foo.check:value > 10")
      val policy = CheckPolicy(1.minute, 2.seconds, 1.minute, 1.minute, None)
      val checkType = "io.mandelbrot.core.check.TimeseriesCheck"
      val factory = CheckBehavior.extensions(checkType).configure(Map("evaluation" -> "probe:foo.check:value > 10"))
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new ObservationBus()

      // create check actor
      val check = system.actorOf(Check.props(checkRef, generation, blackhole, services, metricsBus))

      // initialize check
      check ! ChangeCheck(checkType, policy, factory, Set.empty, 0)

      val getStatus = stateService.expectMsgClass(classOf[GetStatus])
      val status = CheckStatus(generation, DateTime.now(), CheckKnown, None, CheckHealthy,
        Map.empty, None, None, None, None, false)
      stateService.reply(GetStatusResult(getStatus, Some(status)))

      val getObservationHistory = stateService.expectMsgClass(classOf[GetObservationHistory])
      stateService.reply(StateServiceOperationFailed(getObservationHistory, ApiException(ResourceNotFound)))

      // check sets its lifecycle to known
      val updateStatus1 = stateService.expectMsgClass(classOf[UpdateStatus])
      stateService.reply(UpdateStatusResult(updateStatus1))
      updateStatus1.status.lifecycle shouldEqual CheckKnown

      val timestamp = DateTime.now()
      check ! ProcessObservation(ProbeId("foo.check"), ScalarMapObservation(timestamp, Map("value" -> BigDecimal(5))))
      val updateStatus2 = stateService.expectMsgClass(classOf[UpdateStatus])
      stateService.reply(UpdateStatusResult(updateStatus2))
      updateStatus2.status.lifecycle shouldEqual CheckKnown
      updateStatus2.status.health shouldEqual CheckHealthy
      expectMsgClass(classOf[ProcessProbeObservationResult])

      // expiry timer should fire within 5 seconds
      val updateStatus3 = stateService.expectMsgClass(5.seconds, classOf[UpdateStatus])
      updateStatus3.checkRef shouldEqual checkRef
      updateStatus3.status.lifecycle shouldEqual CheckKnown
      updateStatus3.status.health shouldEqual CheckUnknown
      updateStatus3.status.summary shouldEqual None
      updateStatus3.status.correlation shouldEqual Some(_: UUID)
      updateStatus3.status.acknowledged shouldEqual None
      updateStatus3.status.squelched shouldEqual false
    }

    "notify NotificationService when the alert timeout expires" in {
      val checkRef = CheckRef("foo.local:foo.check")
      val policy = CheckPolicy(1.minute, 1.minute, 2.seconds, 1.minute, None)
      val checkType = "io.mandelbrot.core.check.TimeseriesCheck"
      val factory = CheckBehavior.extensions(checkType).configure(Map("evaluation" -> "probe:foo.check:value > 10"))
      val notificationService = new TestProbe(_system)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref), notificationService = Some(notificationService.ref)))
      val metricsBus = new ObservationBus()

      // create check actor
      val check = system.actorOf(Check.props(checkRef, generation, blackhole, services, metricsBus))

      // initialize check
      check ! ChangeCheck(checkType, policy, factory, Set.empty, 0)

      val getStatus = stateService.expectMsgClass(classOf[GetStatus])
      val status = CheckStatus(generation, DateTime.now(), CheckKnown, None, CheckHealthy,
        Map.empty, None, None, None, None, false)
      stateService.reply(GetStatusResult(getStatus, Some(status)))

      val getObservationHistory = stateService.expectMsgClass(classOf[GetObservationHistory])
      stateService.reply(StateServiceOperationFailed(getObservationHistory, ApiException(ResourceNotFound)))

      // check sets its lifecycle to known
      val updateStatus1 = stateService.expectMsgClass(classOf[UpdateStatus])
      stateService.reply(UpdateStatusResult(updateStatus1))
      updateStatus1.status.lifecycle shouldEqual CheckKnown

      val timestamp = DateTime.now()
      check ! ProcessObservation(ProbeId("foo.check"), ScalarMapObservation(timestamp, Map("value" -> BigDecimal(15))))
      val updateStatus2 = stateService.expectMsgClass(classOf[UpdateStatus])
      stateService.reply(UpdateStatusResult(updateStatus2))
      notificationService.expectMsgClass(classOf[NotifyLifecycleChanges])
      notificationService.expectMsgClass(classOf[NotifyHealthChanges])
      expectMsgClass(classOf[ProcessProbeObservationResult])

      // expiry timer should fire within 5 seconds
      val updateStatus3 = stateService.expectMsgClass(5.seconds, classOf[UpdateStatus])
      stateService.reply(UpdateStatusResult(updateStatus3))
      val notification = notificationService.expectMsgClass(classOf[NotifyHealthAlerts])
      notification.checkRef shouldEqual checkRef
      notification.health shouldEqual CheckFailed
      notification.correlation shouldEqual updateStatus2.status.correlation
    }

  }
}
