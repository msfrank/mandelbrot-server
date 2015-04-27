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

package io.mandelbrot.core.system

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
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

class MetricsProbeSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("MetricsProbeSpec", AkkaConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val blackhole = system.actorOf(Blackhole.props())
  val parser = new MetricsEvaluationParser()

  "A Probe with metrics behavior" should {

    "transition to ProbeKnown/ProbeHealthy when a healthy MetricsMessage is received" in {
      val probeRef = ProbeRef("foo.local:foo.check")
      val source = MetricSource(probeRef.checkId, "value")
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.MetricsProbe"
      val factory = ProbeBehavior.extensions(probeType).configure(Map("evaluation" -> "when foo.check:value > 10"))
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val probe = system.actorOf(Probe.props(probeRef, blackhole,  services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory, Set.empty, 0)

      val initializeProbeStatus = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus, Some(status)))

      // probe sets its lifecycle to joining
      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))
      updateProbeStatus1.status.lifecycle shouldEqual ProbeJoining

      val timestamp = DateTime.now()
      probe ! ProcessProbeEvaluation(probeRef, ProbeEvaluation(timestamp, None, None, Some(Map(source.metricName -> BigDecimal(5)))))
      val updateProbeStatus2 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      updateProbeStatus2.status.health shouldEqual ProbeHealthy
      updateProbeStatus2.status.correlation shouldEqual None
      updateProbeStatus2.status.acknowledged shouldEqual None
      updateProbeStatus2.status.squelched shouldEqual false
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus2))

      expectMsgClass(classOf[ProcessProbeEvaluationResult])
    }

    "transition to ProbeKnown/ProbeFailed when a failed MetricsMessage is received" in {
      val probeRef = ProbeRef("foo.local:foo.check")
      val source = MetricSource(probeRef.checkId, "value")
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.MetricsProbe"
      val factory = ProbeBehavior.extensions(probeType).configure(Map("evaluation" -> "when foo.check:value > 10"))
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val probe = system.actorOf(Probe.props(probeRef, blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory, Set.empty, 0)

      val initializeProbeStatus = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus, Some(status)))

      // probe sets its lifecycle to joining
      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))
      updateProbeStatus1.status.lifecycle should be(ProbeJoining)

      val timestamp = DateTime.now()
      probe ! ProcessProbeEvaluation(probeRef, ProbeEvaluation(timestamp, None, None, Some(Map(source.metricName -> BigDecimal(15)))))
      val updateProbeStatus2 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      updateProbeStatus2.status.health shouldEqual ProbeFailed
      updateProbeStatus2.status.correlation shouldEqual Some(_: UUID)
      updateProbeStatus2.status.acknowledged shouldEqual None
      updateProbeStatus2.status.squelched shouldEqual false
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus2))

      expectMsgClass(classOf[ProcessProbeEvaluationResult])
    }

    "notify StateService when the joining timeout expires" in {
      val probeRef = ProbeRef("foo.local:foo.check")
      val source = MetricSource(probeRef.checkId, "value")
      val policy = CheckPolicy(2.seconds, 1.minute, 1.minute, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.MetricsProbe"
      val factory = ProbeBehavior.extensions(probeType).configure(Map("evaluation" -> "when foo.check:value > 10"))
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val probe = system.actorOf(Probe.props(probeRef, blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory, Set.empty, 0)

      val initializeProbeStatus = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus, Some(status)))

      // probe sets its lifecycle to joining
      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))
      updateProbeStatus1.status.lifecycle shouldEqual ProbeJoining

      // expiry timer should fire within 5 seconds
      val updateProbeStatus2 = stateService.expectMsgClass(5.seconds, classOf[UpdateProbeStatus])
      updateProbeStatus2.probeRef shouldEqual probeRef
      updateProbeStatus2.status.health shouldEqual ProbeUnknown
      updateProbeStatus2.status.correlation shouldEqual Some(_: UUID)
      updateProbeStatus2.status.acknowledged shouldEqual None
      updateProbeStatus2.status.squelched shouldEqual false
    }

    "notify StateService when the probe timeout expires" in {
      val probeRef = ProbeRef("foo.local:foo.check")
      val source = MetricSource(probeRef.checkId, "value")
      val evaluation = parser.parseMetricsEvaluation("when foo.check:value > 10")
      val policy = CheckPolicy(1.minute, 2.seconds, 1.minute, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.MetricsProbe"
      val factory = ProbeBehavior.extensions(probeType).configure(Map("evaluation" -> "when foo.check:value > 10"))
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val probe = system.actorOf(Probe.props(probeRef, blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory, Set.empty, 0)

      val initializeProbeStatus = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus, Some(status)))

      // probe sets its lifecycle to joining
      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))
      updateProbeStatus1.status.lifecycle shouldEqual ProbeJoining

      val timestamp = DateTime.now()
      probe ! ProcessProbeEvaluation(probeRef, ProbeEvaluation(timestamp, None, None, Some(Map(source.metricName -> BigDecimal(5)))))
      val updateProbeStatus2 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus2))
      updateProbeStatus2.status.lifecycle shouldEqual ProbeKnown
      updateProbeStatus2.status.health shouldEqual ProbeHealthy
      expectMsgClass(classOf[ProcessProbeEvaluationResult])

      // expiry timer should fire within 5 seconds
      val updateProbeStatus3 = stateService.expectMsgClass(5.seconds, classOf[UpdateProbeStatus])
      updateProbeStatus3.probeRef shouldEqual probeRef
      updateProbeStatus3.status.lifecycle shouldEqual ProbeKnown
      updateProbeStatus3.status.health shouldEqual ProbeUnknown
      updateProbeStatus3.status.summary shouldEqual None
      updateProbeStatus3.status.correlation shouldEqual Some(_: UUID)
      updateProbeStatus3.status.acknowledged shouldEqual None
      updateProbeStatus3.status.squelched shouldEqual false
    }

    "notify NotificationService when the alert timeout expires" in {
      val probeRef = ProbeRef("foo.local:foo.check")
      val source = MetricSource(probeRef.checkId, "value")
      val policy = CheckPolicy(1.minute, 1.minute, 2.seconds, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.MetricsProbe"
      val factory = ProbeBehavior.extensions(probeType).configure(Map("evaluation" -> "when foo.check:value > 10"))
      val notificationService = new TestProbe(_system)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref), notificationService = Some(notificationService.ref)))
      val metricsBus = new MetricsBus()

      val probe = system.actorOf(Probe.props(probeRef, blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory, Set.empty, 0)

      val initializeProbeStatus = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus, Some(status)))

      // probe sets its lifecycle to joining
      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))
      updateProbeStatus1.status.lifecycle shouldEqual ProbeJoining

      val timestamp = DateTime.now()
      probe ! ProcessProbeEvaluation(probeRef, ProbeEvaluation(timestamp, None, None, Some(Map(source.metricName -> BigDecimal(15)))))
      val updateProbeStatus2 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus2))
      notificationService.expectMsgClass(classOf[NotifyLifecycleChanges])
      notificationService.expectMsgClass(classOf[NotifyHealthChanges])
      expectMsgClass(classOf[ProcessProbeEvaluationResult])

      // expiry timer should fire within 5 seconds
      val updateProbeStatus3 = stateService.expectMsgClass(5.seconds, classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus3))
      val notification = notificationService.expectMsgClass(classOf[NotifyHealthAlerts])
      notification.probeRef shouldEqual probeRef
      notification.health shouldEqual ProbeFailed
      notification.correlation shouldEqual updateProbeStatus2.status.correlation
    }

  }
}
