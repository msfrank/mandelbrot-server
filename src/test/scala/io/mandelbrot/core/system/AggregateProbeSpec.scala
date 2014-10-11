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
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import scala.concurrent.duration._
import scala.util.Success

import io.mandelbrot.core.notification._
import io.mandelbrot.core.state.{InitializeProbeState, ProbeState, ProbeStatusCommitted}
import io.mandelbrot.core.{PersistenceConfig, AkkaConfig, Blackhole, ServiceMap}
import io.mandelbrot.core.ConfigConversions._

class AggregateProbeSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpec with MustMatchers with BeforeAndAfterAll {

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
      val services = ServiceMap(blackhole, blackhole, blackhole, blackhole, stateService.ref, blackhole)
      val metricsBus = new MetricsBus()
      val probe = system.actorOf(Probe.props(ref, blackhole, children, initialPolicy, behavior, 0, services, metricsBus))
      stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(ProbeState(status, 0))
      // probe sets its lifecycle to synthetic
      val state = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(state.status, state.lsn))
      state.status.lifecycle must be(ProbeSynthetic)
      val timestamp = DateTime.now()
      probe ! ProbeStatus(child1, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      val result1 = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(result1.status, result1.lsn))
      result1.status.health must be(ProbeUnknown)
      probe ! ProbeStatus(child2, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      val result2 = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(result2.status, result2.lsn))
      result2.status.health must be(ProbeUnknown)
      probe ! ProbeStatus(child3, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      val result3 = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(result3.status, result3.lsn))
      result3.status.health must be(ProbeHealthy)
    }

    "transition to ProbeSynthetic/ProbeDegraded when one child has notified of degraded status" in {
      val ref = ProbeRef("fqdn:local/")
      val behavior = AggregateProbeBehavior(EvaluateWorst, 1.hour, 17)
      val initialPolicy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val children = Set(child1, child2, child3)
      val stateService = new TestProbe(_system)
      val services = ServiceMap(blackhole, blackhole, blackhole, blackhole, stateService.ref, blackhole)
      val metricsBus = new MetricsBus()
      val probe = system.actorOf(Probe.props(ref, blackhole, children, initialPolicy, behavior, 0, services, metricsBus))
      stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(ProbeState(status, 0))
      // probe sets its lifecycle to synthetic
      val state = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(state.status, state.lsn))
      state.status.lifecycle must be(ProbeSynthetic)
      val timestamp = DateTime.now()
      probe ! ProbeStatus(child1, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      val result1 = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(result1.status, result1.lsn))
      result1.status.health must be(ProbeUnknown)
      probe ! ProbeStatus(child2, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      val result2 = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(result2.status, result2.lsn))
      result2.status.health must be(ProbeUnknown)
      probe ! ProbeStatus(child3, timestamp, ProbeKnown, ProbeDegraded, None, None, None, None, None, false)
      val result3 = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(result3.status, result3.lsn))
      result3.status.health must be(ProbeDegraded)
    }

    "transition to ProbeSynthetic/ProbeFailed when one child has notified of failed status" in {
      val ref = ProbeRef("fqdn:local/")
      val behavior = AggregateProbeBehavior(EvaluateWorst, 1.hour, 17)
      val initialPolicy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val children = Set(child1, child2, child3)
      val stateService = new TestProbe(_system)
      val services = ServiceMap(blackhole, blackhole, blackhole, blackhole, stateService.ref, blackhole)
      val metricsBus = new MetricsBus()
      val probe = system.actorOf(Probe.props(ref, blackhole, children, initialPolicy, behavior, 0, services, metricsBus))
      stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(ProbeState(status, 0))
      // probe sets its lifecycle to synthetic
      val state = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(state.status, state.lsn))
      state.status.lifecycle must be(ProbeSynthetic)
      val timestamp = DateTime.now()
      probe ! ProbeStatus(child1, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      val result1 = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(result1.status, result1.lsn))
      result1.status.health must be(ProbeUnknown)
      probe ! ProbeStatus(child2, timestamp, ProbeKnown, ProbeDegraded, None, None, None, None, None, false)
      val result2 = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(result2.status, result2.lsn))
      result2.status.health must be(ProbeUnknown)
      probe ! ProbeStatus(child3, timestamp, ProbeKnown, ProbeFailed, None, None, None, None, None, false)
      val result3 = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(result3.status, result3.lsn))
      result3.status.health must be(ProbeFailed)
    }

    "notify NotificationService when the alert timeout expires" in {
      val ref = ProbeRef("fqdn:local/")
      val behavior = AggregateProbeBehavior(EvaluateWorst, 1.hour, 17)
      val initialPolicy = ProbePolicy(1.minute, 1.minute, 2.seconds, 1.minute, None)
      val children = Set(child1, child2, child3)
      val notificationService = new TestProbe(_system)
      val stateService = new TestProbe(_system)
      val services = ServiceMap(blackhole, blackhole, blackhole, notificationService.ref, stateService.ref, blackhole)
      val metricsBus = new MetricsBus()
      val probe = system.actorOf(Probe.props(ref, blackhole, children, initialPolicy, behavior, 0, services, metricsBus))
      stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(ProbeState(status, 0))
      // probe sets its lifecycle to synthetic
      val state = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(state.status, state.lsn))
      state.status.lifecycle must be(ProbeSynthetic)
      val timestamp = DateTime.now()
      probe ! ProbeStatus(child1, timestamp, ProbeKnown, ProbeFailed, None, None, None, None, None, false)
      val result1 = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(result1.status, result1.lsn))
      result1.status.health must be(ProbeUnknown)
      probe ! ProbeStatus(child2, timestamp, ProbeKnown, ProbeFailed, None, None, None, None, None, false)
      val result2 = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(result2.status, result2.lsn))
      result2.status.health must be(ProbeUnknown)
      probe ! ProbeStatus(child3, timestamp, ProbeKnown, ProbeFailed, None, None, None, None, None, false)
      val result3 = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(result3.status, result3.lsn))
      result3.status.health must be(ProbeFailed)
      notificationService.expectMsgClass(classOf[NotifyHealthChanges])
      // alert timer should fire within 5 seconds
      val result4 = stateService.expectMsgClass(5.seconds, classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(result4.status, result4.lsn))
      val notification = notificationService.expectMsgClass(classOf[NotifyHealthAlerts])
      notification.probeRef must be(ref)
      notification.health must be(ProbeFailed)
      notification.correlation must be === result3.status.correlation
    }

  }
}
