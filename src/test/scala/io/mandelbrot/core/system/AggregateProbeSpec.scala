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
import io.mandelbrot.core.registry.ProbePolicy
import org.joda.time.DateTime
import org.scalatest.matchers.MustMatchers
import org.scalatest.{WordSpecLike, BeforeAndAfterAll, WordSpec}
import scala.concurrent.duration._

import io.mandelbrot.core.notification._
import io.mandelbrot.core.state._
import io.mandelbrot.core.{PersistenceConfig, AkkaConfig, Blackhole}
import io.mandelbrot.core.ConfigConversions._

class AggregateProbeSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("AggregateProbeSpec", AkkaConfig ++ PersistenceConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val child1 = ProbeRef("fqdn:local/child1")
  val child2 = ProbeRef("fqdn:local/child2")
  val child3 = ProbeRef("fqdn:local/child3")
  val blackhole = system.actorOf(Blackhole.props())

  "A Probe with aggregate behavior" must {

    "transition to ProbeSynthetic/ProbeHealthy when all children have notified of healthy status" in {
      val ref = ProbeRef("fqdn:local/")
      val behavior = AggregateProbeBehavior(EvaluateWorst, 1.hour, 17)
      val initialPolicy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val children = Set(child1, child2, child3)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()
      val probe = system.actorOf(Probe.props(ref, blackhole, children, initialPolicy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(InitializeProbeStateResult(initialize, status, 0))
      // probe sets its lifecycle to synthetic
      val update1 = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update1))
      update1.status.lifecycle must be(ProbeSynthetic)
      val timestamp = DateTime.now()
      probe ! ProbeStatus(child1, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      val update2 = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update2))
      update2.status.health must be(ProbeUnknown)
      probe ! ProbeStatus(child2, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      val update3 = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update3))
      update3.status.health must be(ProbeUnknown)
      probe ! ProbeStatus(child3, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      val update4 = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update4))
      update4.status.health must be(ProbeHealthy)
    }

    "transition to ProbeSynthetic/ProbeDegraded when one child has notified of degraded status" in {
      val ref = ProbeRef("fqdn:local/")
      val behavior = AggregateProbeBehavior(EvaluateWorst, 1.hour, 17)
      val initialPolicy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val children = Set(child1, child2, child3)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()
      val probe = system.actorOf(Probe.props(ref, blackhole, children, initialPolicy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(InitializeProbeStateResult(initialize, status, 0))
      // probe sets its lifecycle to synthetic
      val update1 = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update1))
      update1.status.lifecycle must be(ProbeSynthetic)
      val timestamp = DateTime.now()
      probe ! ProbeStatus(child1, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      val update2 = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update2))
      update2.status.health must be(ProbeUnknown)
      probe ! ProbeStatus(child2, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      val update3 = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update3))
      update3.status.health must be(ProbeUnknown)
      probe ! ProbeStatus(child3, timestamp, ProbeKnown, ProbeDegraded, None, None, None, None, None, false)
      val update4 = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update4))
      update4.status.health must be(ProbeDegraded)
    }

    "transition to ProbeSynthetic/ProbeFailed when one child has notified of failed status" in {
      val ref = ProbeRef("fqdn:local/")
      val behavior = AggregateProbeBehavior(EvaluateWorst, 1.hour, 17)
      val initialPolicy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val children = Set(child1, child2, child3)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()
      val probe = system.actorOf(Probe.props(ref, blackhole, children, initialPolicy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(InitializeProbeStateResult(initialize, status, 0))
      // probe sets its lifecycle to synthetic
      val update1 = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update1))
      update1.status.lifecycle must be(ProbeSynthetic)
      val timestamp = DateTime.now()
      probe ! ProbeStatus(child1, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      val update2 = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update2))
      update2.status.health must be(ProbeUnknown)
      probe ! ProbeStatus(child2, timestamp, ProbeKnown, ProbeDegraded, None, None, None, None, None, false)
      val update3 = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update3))
      update3.status.health must be(ProbeUnknown)
      probe ! ProbeStatus(child3, timestamp, ProbeKnown, ProbeFailed, None, None, None, None, None, false)
      val update4 = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update4))
      update4.status.health must be(ProbeFailed)
    }

    "notify NotificationService when the alert timeout expires" in {
      val ref = ProbeRef("fqdn:local/")
      val behavior = AggregateProbeBehavior(EvaluateWorst, 1.hour, 17)
      val initialPolicy = ProbePolicy(1.minute, 1.minute, 2.seconds, 1.minute, None)
      val children = Set(child1, child2, child3)
      val notificationService = new TestProbe(_system)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref), notificationService = Some(notificationService.ref)))
      val metricsBus = new MetricsBus()
      val probe = system.actorOf(Probe.props(ref, blackhole, children, initialPolicy, behavior, 0, services, metricsBus))
      val initialize = stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(InitializeProbeStateResult(initialize, status, 0))
      // probe sets its lifecycle to synthetic
      val update1 = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update1))
      update1.status.lifecycle must be(ProbeSynthetic)
      val timestamp = DateTime.now()
      probe ! ProbeStatus(child1, timestamp, ProbeKnown, ProbeFailed, None, None, None, None, None, false)
      val update2 = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update2))
      update2.status.health must be(ProbeUnknown)
      probe ! ProbeStatus(child2, timestamp, ProbeKnown, ProbeFailed, None, None, None, None, None, false)
      val update3 = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update3))
      update3.status.health must be(ProbeUnknown)
      probe ! ProbeStatus(child3, timestamp, ProbeKnown, ProbeFailed, None, None, None, None, None, false)
      val update4 = stateService.expectMsgClass(classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update4))
      update4.status.health must be(ProbeFailed)
      notificationService.expectMsgClass(classOf[NotifyHealthChanges])
      // alert timer should fire within 5 seconds
      val update5 = stateService.expectMsgClass(5.seconds, classOf[UpdateProbeState])
      stateService.reply(UpdateProbeStateResult(update5))
      val notification = notificationService.expectMsgClass(classOf[NotifyHealthAlerts])
      notification.probeRef must be(ref)
      notification.health must be(ProbeFailed)
      notification.correlation must be === update4.status.correlation
    }

  }
}
