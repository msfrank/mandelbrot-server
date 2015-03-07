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
import io.mandelbrot.core.metrics.MetricsBus
import org.joda.time.DateTime
import org.scalatest.ShouldMatchers
import org.scalatest.{WordSpecLike, BeforeAndAfterAll}
import scala.concurrent.duration._

import io.mandelbrot.core.notification._
import io.mandelbrot.core.registry.ProbePolicy
import io.mandelbrot.core.state._
import io.mandelbrot.core.{PersistenceConfig, AkkaConfig, Blackhole}
import io.mandelbrot.core.ConfigConversions._

class ScalarProbeSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ScalarProbeSpec", AkkaConfig ++ PersistenceConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val blackhole = system.actorOf(Blackhole.props())

  "A Probe with scalar behavior" should {

    "transition to ProbeKnown/ProbeHealthy when a healthy StatusMessage is received" in {
      val probeRef = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val actor = system.actorOf(Probe.props(probeRef, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeJoining, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initialize, Some(status)))

      val timestamp = DateTime.now()
      actor ! ProcessProbeEvaluation(probeRef, ProbeEvaluation(timestamp, Some("healthy"), Some(ProbeHealthy), None))
      val update1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      update1.status.lifecycle shouldEqual ProbeKnown
      update1.status.health shouldEqual ProbeHealthy
      update1.status.summary shouldEqual Some("healthy")
      update1.status.correlation shouldEqual None
      update1.status.acknowledged shouldEqual None
      update1.status.squelched shouldEqual false
      stateService.reply(UpdateProbeStatusResult(update1))

      expectMsgClass(classOf[ProcessProbeEvaluationResult])
    }

    "transition to ProbeKnown/ProbeDegraded when a degraded StatusMessage is received" in {
      val probeRef = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val actor = system.actorOf(Probe.props(probeRef, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeJoining, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initialize, Some(status)))

      val timestamp = DateTime.now()
      actor ! ProcessProbeEvaluation(probeRef, ProbeEvaluation(timestamp, Some("degraded"), Some(ProbeDegraded), None))
      val update1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      update1.status.lifecycle shouldEqual ProbeKnown
      update1.status.health shouldEqual ProbeDegraded
      update1.status.summary shouldEqual Some("degraded")
      update1.status.correlation shouldEqual Some(_: UUID)
      update1.status.acknowledged shouldEqual None
      update1.status.squelched shouldEqual false
      stateService.reply(UpdateProbeStatusResult(update1))

      expectMsgClass(classOf[ProcessProbeEvaluationResult])
    }

    "transition to ProbeKnown/ProbeFailed when a failed StatusMessage is received" in {
      val probeRef = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val actor = system.actorOf(Probe.props(probeRef, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeJoining, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initialize, Some(status)))

      val timestamp = DateTime.now()
      actor ! ProcessProbeEvaluation(probeRef, ProbeEvaluation(timestamp, Some("failed"), Some(ProbeFailed), None))
      val update1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      update1.status.lifecycle shouldEqual ProbeKnown
      update1.status.health shouldEqual ProbeFailed
      update1.status.summary shouldEqual Some("failed")
      update1.status.correlation shouldEqual Some(_: UUID)
      update1.status.acknowledged shouldEqual None
      update1.status.squelched shouldEqual false
      stateService.reply(UpdateProbeStatusResult(update1))

      expectMsgClass(classOf[ProcessProbeEvaluationResult])
    }

    "transition to ProbeKnown/ProbeUnknown when a unknown StatusMessage is received" in {
      val probeRef = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val actor = system.actorOf(Probe.props(probeRef, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeJoining, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initialize, Some(status)))

      val timestamp = DateTime.now()
      actor ! ProcessProbeEvaluation(probeRef, ProbeEvaluation(timestamp, Some("unknown"), Some(ProbeUnknown), None))
      val update1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      update1.status.lifecycle shouldEqual ProbeKnown
      update1.status.health shouldEqual ProbeUnknown
      update1.status.summary shouldEqual Some("unknown")
      update1.status.correlation shouldEqual Some(_: UUID)
      update1.status.acknowledged shouldEqual None
      update1.status.squelched shouldEqual false
      stateService.reply(UpdateProbeStatusResult(update1))

      expectMsgClass(classOf[ProcessProbeEvaluationResult])
    }

    "notify StateService when the joining timeout expires" in {
      val probeRef = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(2.seconds, 1.minute, 1.minute, 1.minute, None)
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val actor = system.actorOf(Probe.props(probeRef, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeJoining, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initialize, Some(status)))

      // expiry timer should fire within 5 seconds
      val update1 = stateService.expectMsgClass(5.seconds, classOf[UpdateProbeStatus])
      update1.probeRef shouldEqual probeRef
      update1.status.lifecycle shouldEqual ProbeJoining
      update1.status.health shouldEqual ProbeUnknown
      update1.status.summary shouldEqual None
      update1.status.correlation shouldEqual Some(_: UUID)
      update1.status.acknowledged shouldEqual None
      update1.status.squelched shouldEqual false
    }

    "notify StateService when the probe timeout expires" in {
      val probeRef = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 2.seconds, 1.minute, 1.minute, None)
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val actor = system.actorOf(Probe.props(probeRef, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeJoining, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initialize, Some(status)))

      val timestamp = DateTime.now()
      actor ! ProcessProbeEvaluation(probeRef, ProbeEvaluation(timestamp, Some("healthy"), Some(ProbeHealthy), None))
      val update1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(update1))

      // expiry timer should fire within 5 seconds
      val update2 = stateService.expectMsgClass(5.seconds, classOf[UpdateProbeStatus])
      update2.probeRef shouldEqual probeRef
      update2.status.lifecycle shouldEqual ProbeKnown
      update2.status.health shouldEqual ProbeUnknown
      update2.status.summary shouldEqual None
      update2.status.correlation shouldEqual Some(_: UUID)
      update2.status.acknowledged shouldEqual None
      update2.status.squelched shouldEqual false
    }

    "notify NotificationService when the alert timeout expires" in {
      val probeRef = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 1.minute, 2.seconds, 1.minute, None)
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val notificationService = new TestProbe(_system)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref), notificationService = Some(notificationService.ref)))
      val metricsBus = new MetricsBus()

      val actor = system.actorOf(Probe.props(probeRef, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeJoining, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initialize, Some(status)))

      val timestamp = DateTime.now()
      actor ! ProcessProbeEvaluation(probeRef, ProbeEvaluation(timestamp, Some("failed"), Some(ProbeFailed), None))
      val update1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(update1))
      notificationService.expectMsgClass(classOf[NotifyLifecycleChanges])
      notificationService.expectMsgClass(classOf[NotifyHealthChanges])

      // alert timer should fire within 5 seconds
      val update2 = stateService.expectMsgClass(5.seconds, classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(update2))
      val notification = notificationService.expectMsgClass(8.seconds, classOf[NotifyHealthAlerts])
      notification.probeRef shouldEqual probeRef
      notification.health shouldEqual ProbeFailed
      notification.correlation shouldEqual update1.status.correlation
    }

  }
}

