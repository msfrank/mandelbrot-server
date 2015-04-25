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

import io.mandelbrot.core.model._
import io.mandelbrot.core.state._
import io.mandelbrot.core.metrics._
import io.mandelbrot.core.{AkkaConfig, Blackhole}
import io.mandelbrot.core.ConfigConversions._

class ScalarProbeSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ScalarProbeSpec", AkkaConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val blackhole = system.actorOf(Blackhole.props())

  "A Probe with scalar behavior" should {

    "transition to ProbeKnown/ProbeHealthy when a healthy StatusMessage is received" in {
      val probeRef = ProbeRef("fqdn:local/")
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.ScalarProbe"
      val factory = ProbeBehavior.extensions(probeType).configure(Map.empty)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val probe = system.actorOf(Probe.props(probeRef, blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory, Set.empty, 0)

      val initializeProbeStatus = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus, Some(status)))

      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      updateProbeStatus1.status.lifecycle shouldEqual ProbeJoining
      updateProbeStatus1.status.health shouldEqual ProbeUnknown
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))

      val timestamp = DateTime.now()
      probe ! ProcessProbeEvaluation(probeRef, ProbeEvaluation(timestamp, Some("healthy"), Some(ProbeHealthy), None))
      val updateProbeStatus2 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      updateProbeStatus2.status.lifecycle shouldEqual ProbeKnown
      updateProbeStatus2.status.health shouldEqual ProbeHealthy
      updateProbeStatus2.status.summary shouldEqual Some("healthy")
      updateProbeStatus2.status.correlation shouldEqual None
      updateProbeStatus2.status.acknowledged shouldEqual None
      updateProbeStatus2.status.squelched shouldEqual false
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus2))

      expectMsgClass(classOf[ProcessProbeEvaluationResult])
    }

    "transition to ProbeKnown/ProbeDegraded when a degraded StatusMessage is received" in {
      val probeRef = ProbeRef("fqdn:local/")
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.ScalarProbe"
      val factory = ProbeBehavior.extensions(probeType).configure(Map.empty)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val probe = system.actorOf(Probe.props(probeRef, blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory, Set.empty, 0)

      val initializeProbeStatus = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus, Some(status)))

      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      updateProbeStatus1.status.lifecycle shouldEqual ProbeJoining
      updateProbeStatus1.status.health shouldEqual ProbeUnknown
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))

      val timestamp = DateTime.now()
      probe ! ProcessProbeEvaluation(probeRef, ProbeEvaluation(timestamp, Some("degraded"), Some(ProbeDegraded), None))
      val updateProbeStatus2 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      updateProbeStatus2.status.lifecycle shouldEqual ProbeKnown
      updateProbeStatus2.status.health shouldEqual ProbeDegraded
      updateProbeStatus2.status.summary shouldEqual Some("degraded")
      updateProbeStatus2.status.correlation shouldEqual Some(_: UUID)
      updateProbeStatus2.status.acknowledged shouldEqual None
      updateProbeStatus2.status.squelched shouldEqual false
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus2))

      expectMsgClass(classOf[ProcessProbeEvaluationResult])
    }

    "transition to ProbeKnown/ProbeFailed when a failed StatusMessage is received" in {
      val probeRef = ProbeRef("fqdn:local/")
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.ScalarProbe"
      val factory = ProbeBehavior.extensions(probeType).configure(Map.empty)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val probe = system.actorOf(Probe.props(probeRef, blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory, Set.empty, 0)

      val initializeProbeStatus = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus, Some(status)))

      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      updateProbeStatus1.status.lifecycle shouldEqual ProbeJoining
      updateProbeStatus1.status.health shouldEqual ProbeUnknown
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))

      val timestamp = DateTime.now()
      probe ! ProcessProbeEvaluation(probeRef, ProbeEvaluation(timestamp, Some("failed"), Some(ProbeFailed), None))
      val updateProbeStatus2 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      updateProbeStatus2.status.lifecycle shouldEqual ProbeKnown
      updateProbeStatus2.status.health shouldEqual ProbeFailed
      updateProbeStatus2.status.summary shouldEqual Some("failed")
      updateProbeStatus2.status.correlation shouldEqual Some(_: UUID)
      updateProbeStatus2.status.acknowledged shouldEqual None
      updateProbeStatus2.status.squelched shouldEqual false
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus2))

      expectMsgClass(classOf[ProcessProbeEvaluationResult])
    }

    "transition to ProbeKnown/ProbeUnknown when a unknown StatusMessage is received" in {
      val probeRef = ProbeRef("fqdn:local/")
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.ScalarProbe"
      val factory = ProbeBehavior.extensions(probeType).configure(Map.empty)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val probe = system.actorOf(Probe.props(probeRef, blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory, Set.empty, 0)

      val initializeProbeStatus = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus, Some(status)))

      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      updateProbeStatus1.status.lifecycle shouldEqual ProbeJoining
      updateProbeStatus1.status.health shouldEqual ProbeUnknown
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))

      val timestamp = DateTime.now()
      probe ! ProcessProbeEvaluation(probeRef, ProbeEvaluation(timestamp, Some("unknown"), Some(ProbeUnknown), None))
      val updateProbeStatus2 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      updateProbeStatus2.status.lifecycle shouldEqual ProbeKnown
      updateProbeStatus2.status.health shouldEqual ProbeUnknown
      updateProbeStatus2.status.summary shouldEqual Some("unknown")
      updateProbeStatus2.status.correlation shouldEqual Some(_: UUID)
      updateProbeStatus2.status.acknowledged shouldEqual None
      updateProbeStatus2.status.squelched shouldEqual false
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus2))

      expectMsgClass(classOf[ProcessProbeEvaluationResult])
    }

    "notify StateService when the joining timeout expires" in {
      val probeRef = ProbeRef("fqdn:local/")
      val policy = CheckPolicy(2.seconds, 1.minute, 1.minute, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.ScalarProbe"
      val factory = ProbeBehavior.extensions(probeType).configure(Map.empty)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val probe = system.actorOf(Probe.props(probeRef, blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory, Set.empty, 0)

      val initializeProbeStatus = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus, Some(status)))

      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      updateProbeStatus1.status.lifecycle shouldEqual ProbeJoining
      updateProbeStatus1.status.health shouldEqual ProbeUnknown
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))

      // expiry timer should fire within 5 seconds
      val updateProbeStatus2 = stateService.expectMsgClass(5.seconds, classOf[UpdateProbeStatus])
      updateProbeStatus2.probeRef shouldEqual probeRef
      updateProbeStatus2.status.lifecycle shouldEqual ProbeJoining
      updateProbeStatus2.status.health shouldEqual ProbeUnknown
      updateProbeStatus2.status.summary shouldEqual None
      updateProbeStatus2.status.correlation shouldEqual Some(_: UUID)
      updateProbeStatus2.status.acknowledged shouldEqual None
      updateProbeStatus2.status.squelched shouldEqual false
    }

    "notify StateService when the probe timeout expires" in {
      val probeRef = ProbeRef("fqdn:local/")
      val policy = CheckPolicy(1.minute, 2.seconds, 1.minute, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.ScalarProbe"
      val factory = ProbeBehavior.extensions(probeType).configure(Map.empty)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val probe = system.actorOf(Probe.props(probeRef, blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory, Set.empty, 0)

      val initializeProbeStatus = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus, Some(status)))

      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      updateProbeStatus1.status.lifecycle shouldEqual ProbeJoining
      updateProbeStatus1.status.health shouldEqual ProbeUnknown
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))

      val timestamp = DateTime.now()
      probe ! ProcessProbeEvaluation(probeRef, ProbeEvaluation(timestamp, Some("healthy"), Some(ProbeHealthy), None))
      val updateProbeStatus2 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus2))

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
      val probeRef = ProbeRef("fqdn:local/")
      val policy = CheckPolicy(1.minute, 1.minute, 2.seconds, 1.minute, None)
      val probeType = "io.mandelbrot.core.system.ScalarProbe"
      val factory = ProbeBehavior.extensions(probeType).configure(Map.empty)
      val notificationService = new TestProbe(_system)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref), notificationService = Some(notificationService.ref)))
      val metricsBus = new MetricsBus()

      val probe = system.actorOf(Probe.props(probeRef, blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory, Set.empty, 0)

      val initializeProbeStatus = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus, Some(status)))

      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      updateProbeStatus1.status.lifecycle shouldEqual ProbeJoining
      updateProbeStatus1.status.health shouldEqual ProbeUnknown
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))

      val timestamp = DateTime.now()
      probe ! ProcessProbeEvaluation(probeRef, ProbeEvaluation(timestamp, Some("failed"), Some(ProbeFailed), None))
      val updateProbeStatus2 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus2))
      notificationService.expectMsgClass(classOf[NotifyLifecycleChanges])
      notificationService.expectMsgClass(classOf[NotifyHealthChanges])

      // alert timer should fire within 5 seconds
      val updateProbeStatus3 = stateService.expectMsgClass(5.seconds, classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus3))
      val notification = notificationService.expectMsgClass(8.seconds, classOf[NotifyHealthAlerts])
      notification.probeRef shouldEqual probeRef
      notification.health shouldEqual ProbeFailed
      notification.correlation shouldEqual updateProbeStatus2.status.correlation
    }

  }
}

