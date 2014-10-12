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
import io.mandelbrot.core.metrics.{EvaluateSource, HeadFunction, ValueGreaterThan, MetricsEvaluation}
import org.joda.time.DateTime
import org.scalatest.matchers.MustMatchers
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import scala.concurrent.duration._
import scala.math.BigDecimal
import scala.util.Success

import io.mandelbrot.core.notification._
import io.mandelbrot.core.metrics._
import io.mandelbrot.core.registry.ProbePolicy
import io.mandelbrot.core.state.{InitializeProbeState, ProbeState, ProbeStatusCommitted}
import io.mandelbrot.core.{PersistenceConfig, AkkaConfig, Blackhole, ServiceMap}
import io.mandelbrot.core.ConfigConversions._

class MetricsProbeSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpec with MustMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("MetricsProbeSpec", AkkaConfig ++ PersistenceConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val blackhole = system.actorOf(Blackhole.props())
  val parser = new MetricsEvaluationParser()

  "A Probe with metrics behavior" must {

    "transition to ProbeKnown/ProbeHealthy when a healthy MetricsMessage is received" in {
      val ref = ProbeRef("fqdn:local/")
      val source = MetricSource(Vector.empty, "foo")
      val evaluation = parser.parseMetricsEvaluation("foo > 10")
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val behavior = MetricsProbeBehavior(evaluation, 1.hour, 17)
      val stateService = new TestProbe(_system)
      val services = ServiceMap(blackhole, blackhole, blackhole, blackhole, stateService.ref, blackhole)
      val metricsBus = new MetricsBus()
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(ProbeState(status, 0))
      // probe sets its lifecycle to joining
      val state = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(state.status, state.lsn))
      state.status.lifecycle must be(ProbeJoining)
      val timestamp = DateTime.now()
      actor ! MetricsMessage(ref, Map(source.metricName -> BigDecimal(5)), timestamp)
      val result = stateService.expectMsgClass(classOf[ProbeState])
      result.status.health must be(ProbeHealthy)
      result.status.correlation must be(None)
      result.status.acknowledged must be(None)
      result.status.squelched must be(false)
    }

    "transition to ProbeKnown/ProbeFailed when a failed MetricsMessage is received" in {
      val ref = ProbeRef("fqdn:local/")
      val source = MetricSource(Vector.empty, "foo")
      val evaluation = parser.parseMetricsEvaluation("foo > 10")
      val policy = ProbePolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val behavior = MetricsProbeBehavior(evaluation, 1.hour, 17)
      val stateService = new TestProbe(_system)
      val services = ServiceMap(blackhole, blackhole, blackhole, blackhole, stateService.ref, blackhole)
      val metricsBus = new MetricsBus()
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(ProbeState(status, 0))
      // probe sets its lifecycle to joining
      val state = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(state.status, state.lsn))
      state.status.lifecycle must be(ProbeJoining)
      val timestamp = DateTime.now()
      actor ! MetricsMessage(ref, Map(source.metricName -> BigDecimal(15)), timestamp)
      val result = stateService.expectMsgClass(classOf[ProbeState])
      result.status.health must be(ProbeFailed)
      result.status.correlation must not be(None)
      result.status.acknowledged must be(None)
      result.status.squelched must be(false)
    }

    "notify StateService when the joining timeout expires" in {
      val ref = ProbeRef("fqdn:local/")
      val source = MetricSource(Vector.empty, "foo")
      val evaluation = parser.parseMetricsEvaluation("foo > 10")
      val policy = ProbePolicy(2.seconds, 1.minute, 1.minute, 1.minute, None)
      val behavior = MetricsProbeBehavior(evaluation, 1.hour, 17)
      val stateService = new TestProbe(_system)
      val services = ServiceMap(blackhole, blackhole, blackhole, blackhole, stateService.ref, blackhole)
      val metricsBus = new MetricsBus()
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(ProbeState(status, 0))
      // probe sets its lifecycle to joining
      val state = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(state.status, state.lsn))
      state.status.lifecycle must be(ProbeJoining)
      // expiry timer should fire within 5 seconds
      val result = stateService.expectMsgClass(5.seconds, classOf[ProbeState])
      result.status.probeRef must be(ref)
      result.status.health must be(ProbeUnknown)
      result.status.summary must be(None)
      result.status.correlation must not be(None)
      result.status.acknowledged must be(None)
      result.status.squelched must be(false)
    }

    "notify StateService when the probe timeout expires" in {
      val ref = ProbeRef("fqdn:local/")
      val source = MetricSource(Vector.empty, "foo")
      val evaluation = parser.parseMetricsEvaluation("foo > 10")
      val policy = ProbePolicy(1.minute, 2.seconds, 1.minute, 1.minute, None)
      val behavior = MetricsProbeBehavior(evaluation, 1.hour, 17)
      val stateService = new TestProbe(_system)
      val services = ServiceMap(blackhole, blackhole, blackhole, blackhole, stateService.ref, blackhole)
      val metricsBus = new MetricsBus()
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(ProbeState(status, 0))
      // probe sets its lifecycle to joining
      val initial = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(initial.status, initial.lsn))
      initial.status.lifecycle must be(ProbeJoining)
      val timestamp = DateTime.now()
      actor ! MetricsMessage(ref, Map(source.metricName -> BigDecimal(5)), timestamp)
      val state = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(state.status, state.lsn))
      state.status.lifecycle must be(ProbeKnown)
      state.status.health must be(ProbeHealthy)
      // expiry timer should fire within 5 seconds
      val result = stateService.expectMsgClass(5.seconds, classOf[ProbeState])
      result.status.probeRef must be(ref)
      result.status.lifecycle must be(ProbeKnown)
      result.status.health must be(ProbeUnknown)
      result.status.summary must be(None)
      result.status.correlation must not be(None)
      result.status.acknowledged must be(None)
      result.status.squelched must be(false)
    }

    "notify NotificationService when the alert timeout expires" in {
      val ref = ProbeRef("fqdn:local/")
      val source = MetricSource(Vector.empty, "foo")
      val evaluation = parser.parseMetricsEvaluation("foo > 10")
      val policy = ProbePolicy(1.minute, 1.minute, 2.seconds, 1.minute, None)
      val behavior = MetricsProbeBehavior(evaluation, 1.hour, 17)
      val notificationService = new TestProbe(_system)
      val stateService = new TestProbe(_system)
      val services = ServiceMap(blackhole, blackhole, blackhole, notificationService.ref, stateService.ref, blackhole)
      val metricsBus = new MetricsBus()
      val actor = system.actorOf(Probe.props(ref, blackhole, Set.empty, policy, behavior, 0, services, metricsBus))
      stateService.expectMsgClass(classOf[InitializeProbeState])
      val status = ProbeStatus(ref, DateTime.now(), ProbeInitializing, ProbeUnknown, None, None, None, None, None, false)
      stateService.reply(ProbeState(status, 0))
      // probe sets its lifecycle to joining
      val initial = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(initial.status, initial.lsn))
      initial.status.lifecycle must be(ProbeJoining)
      val timestamp = DateTime.now()
      actor ! MetricsMessage(ref, Map(source.metricName -> BigDecimal(15)), timestamp)
      val state = stateService.expectMsgClass(classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(state.status, state.lsn))
      notificationService.expectMsgClass(classOf[NotifyLifecycleChanges])
      notificationService.expectMsgClass(classOf[NotifyHealthChanges])
      // expiry timer should fire within 5 seconds
      val result = stateService.expectMsgClass(5.seconds, classOf[ProbeState])
      stateService.reply(ProbeStatusCommitted(result.status, result.lsn))
      val notification = notificationService.expectMsgClass(classOf[NotifyHealthAlerts])
      notification.probeRef must be(ref)
      notification.health must be(ProbeFailed)
      notification.correlation must be === state.status.correlation
    }

  }
}
