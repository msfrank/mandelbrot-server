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

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import io.mandelbrot.core.metrics.MetricsBus
import org.joda.time.DateTime
import org.scalatest.matchers.MustMatchers
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import scala.concurrent.duration._

import io.mandelbrot.core.notification._
import io.mandelbrot.core.registry.ProbePolicy
import io.mandelbrot.core.state._
import io.mandelbrot.core.{PersistenceConfig, AkkaConfig, Blackhole}
import io.mandelbrot.core.ConfigConversions._

class ScalarProbeSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpec with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ScalarProbeSpec", AkkaConfig ++ PersistenceConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val blackhole = system.actorOf(Blackhole.props())

  "A Probe with scalar behavior" must {

    "transition to ProbeKnown/ProbeHealthy when a healthy StatusMessage is received" in {
      val ref = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeJoining, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(InitializeProbeStateResult(initialize, status, 0))
      val timestamp = DateTime.now()
      actor ! StatusMessage(ref, ProbeHealthy, "healthy", None, timestamp)
      val update1 = stateService.expectMsgClass(classOf[UpdateProbeState])
      update1.status.lifecycle must be(ProbeKnown)
      update1.status.health must be(ProbeHealthy)
      update1.status.summary must be(Some("healthy"))
      update1.status.correlation must be(None)
      update1.status.acknowledged must be(None)
      update1.status.squelched must be(false)
    }

    "transition to ProbeKnown/ProbeDegraded when a degraded StatusMessage is received" in {
      val ref = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeJoining, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(InitializeProbeStateResult(initialize, status, 0))
      val timestamp = DateTime.now()
      actor ! StatusMessage(ref, ProbeDegraded, "degraded", None, timestamp)
      val update1 = stateService.expectMsgClass(classOf[UpdateProbeState])
      update1.status.lifecycle must be(ProbeKnown)
      update1.status.health must be(ProbeDegraded)
      update1.status.summary must be(Some("degraded"))
      update1.status.correlation must not be(None)
      update1.status.acknowledged must be(None)
      update1.status.squelched must be(false)
    }

    "transition to ProbeKnown/ProbeFailed when a failed StatusMessage is received" in {
      val ref = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeJoining, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(InitializeProbeStateResult(initialize, status, 0))
      val timestamp = DateTime.now()
      actor ! StatusMessage(ref, ProbeFailed, "failed", None, timestamp)
      val update1 = stateService.expectMsgClass(classOf[UpdateProbeState])
      update1.status.lifecycle must be(ProbeKnown)
      update1.status.health must be(ProbeFailed)
      update1.status.summary must be(Some("failed"))
      update1.status.correlation must not be(None)
      update1.status.acknowledged must be(None)
      update1.status.squelched must be(false)
    }

    "transition to ProbeKnown/ProbeUnknown when a unknown StatusMessage is received" in {
      val ref = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeJoining, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(InitializeProbeStateResult(initialize, status, 0))
      val timestamp = DateTime.now()
      actor ! StatusMessage(ref, ProbeUnknown, "unknown", None, timestamp)
      val update1 = stateService.expectMsgClass(classOf[UpdateProbeState])
      update1.status.lifecycle must be(ProbeKnown)
      update1.status.health must be(ProbeUnknown)
      update1.status.summary must be(Some("unknown"))
      update1.status.correlation must not be(None)
      update1.status.acknowledged must be(None)
      update1.status.squelched must be(false)
    }

    "notify StateService when the joining timeout expires" in {
      val ref = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(2.seconds, 1.minute, 1.minute, 1.minute, None)
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeJoining, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(InitializeProbeStateResult(initialize, status, 0))
      // expiry timer should fire within 5 seconds
      val update1 = stateService.expectMsgClass(5.seconds, classOf[UpdateProbeState])
      update1.status.probeRef must be(ref)
      update1.status.lifecycle must be(ProbeJoining)
      update1.status.health must be(ProbeUnknown)
      update1.status.summary must be(None)
      update1.status.correlation must not be(None)
      update1.status.acknowledged must be(None)
      update1.status.squelched must be(false)
    }

    "notify StateService when the probe timeout expires" in {
      val ref = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 2.seconds, 1.minute, 1.minute, None)
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeJoining, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(InitializeProbeStateResult(initialize, status, 0))
      val timestamp = DateTime.now()
      actor ! StatusMessage(ref, ProbeHealthy, "healthy", None, timestamp)
      val update1 = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update1))
      // expiry timer should fire within 5 seconds
      val update2 = stateService.expectMsgClass(5.seconds, classOf[UpdateProbeState])
      update2.status.probeRef must be(ref)
      update2.status.lifecycle must be(ProbeKnown)
      update2.status.health must be(ProbeUnknown)
      update2.status.summary must be(None)
      update2.status.correlation must not be(None)
      update2.status.acknowledged must be(None)
      update2.status.squelched must be(false)
    }

    "notify NotificationService when the alert timeout expires" in {
      val ref = ProbeRef("fqdn:local/")
      val policy = ProbePolicy(1.minute, 1.minute, 2.seconds, 1.minute, None)
      val behavior = ScalarProbeBehavior(1.hour, 17)
      val notificationService = new TestProbe(_system)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref), notificationService = Some(notificationService.ref)))
      val metricsBus = new MetricsBus()
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeJoining, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(InitializeProbeStateResult(initialize, status, 0))
      val timestamp = DateTime.now()
      actor ! StatusMessage(ref, ProbeFailed, "failed", None, timestamp)
      val update1 = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update1))
      notificationService.expectMsgClass(classOf[NotifyLifecycleChanges])
      notificationService.expectMsgClass(classOf[NotifyHealthChanges])
      // alert timer should fire within 5 seconds
      val update2 = stateService.expectMsgClass(5.seconds, classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update2))
      val notification = notificationService.expectMsgClass(8.seconds, classOf[NotifyHealthAlerts])
      notification.probeRef must be(ref)
      notification.health must be(ProbeFailed)
      notification.correlation must be === update1.status.correlation
    }

  }
}

