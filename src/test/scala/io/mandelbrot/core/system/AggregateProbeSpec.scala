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
import org.joda.time.DateTime
import org.scalatest.ShouldMatchers
import org.scalatest.{WordSpecLike, BeforeAndAfterAll}
import scala.concurrent.duration._

import io.mandelbrot.core.metrics.MetricsBus
import io.mandelbrot.core.state._
import io.mandelbrot.core.model._
import io.mandelbrot.core.{AkkaConfig, Blackhole}
import io.mandelbrot.core.ConfigConversions._

class AggregateProbeSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("AggregateProbeSpec", AkkaConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val child1 = ProbeRef("foo.local:check.child1")
  val child2 = ProbeRef("foo.local:check.child2")
  val child3 = ProbeRef("foo.local:check.child3")
  val blackhole = system.actorOf(Blackhole.props())

  "A Probe with aggregate behavior" should {

    "transition to ProbeSynthetic/ProbeHealthy when all children have notified of healthy status" in {
      val probeRef = ProbeRef("foo.local:check")
      val probeType = "io.mandelbrot.core.system.AggregateProbe"
      val factory = ProbeBehavior.extensions(probeType).configure(Map.empty)
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val children = Set(child1, child2, child3)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val probe = system.actorOf(Probe.props(probeRef, blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory, children, 0)

      val initializeProbeStatus = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus, Some(status)))

      // probe sets its lifecycle to synthetic
      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))
      updateProbeStatus1.status.lifecycle shouldEqual ProbeSynthetic
      updateProbeStatus1.status.health shouldEqual ProbeUnknown

      val timestamp = DateTime.now()

      probe ! ChildMutates(child1, ProbeStatus(timestamp, ProbeKnown, None, ProbeHealthy, Map.empty, None, None, None, None, false))
      val updateProbeStatus2 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus2))
      updateProbeStatus2.status.health shouldEqual ProbeUnknown

      probe ! ChildMutates(child2, ProbeStatus(timestamp, ProbeKnown, None, ProbeHealthy, Map.empty, None, None, None, None, false))
      val updateProbeStatus3 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus3))
      updateProbeStatus3.status.health shouldEqual ProbeUnknown

      probe ! ChildMutates(child3, ProbeStatus(timestamp, ProbeKnown, None, ProbeHealthy, Map.empty, None, None, None, None, false))
      val updateProbeStatus4 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus4))
      updateProbeStatus4.status.health shouldEqual ProbeHealthy
    }

    "transition to ProbeSynthetic/ProbeDegraded when one child has notified of degraded status" in {
      val probeRef = ProbeRef("foo.local:check")
      val probeType = "io.mandelbrot.core.system.AggregateProbe"
      val factory = ProbeBehavior.extensions(probeType).configure(Map.empty)
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val children = Set(child1, child2, child3)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val probe = system.actorOf(Probe.props(probeRef, blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory, children, 0)

      val initializeProbeStatus = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus, Some(status)))

      // probe sets its lifecycle to synthetic
      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))
      updateProbeStatus1.status.lifecycle shouldEqual ProbeSynthetic

      val timestamp = DateTime.now()

      probe ! ChildMutates(child1, ProbeStatus(timestamp, ProbeKnown, None, ProbeHealthy, Map.empty, None, None, None, None, false))
      val updateProbeStatus2 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus2))
      updateProbeStatus2.status.health shouldEqual ProbeUnknown

      probe ! ChildMutates(child2, ProbeStatus(timestamp, ProbeKnown, None, ProbeHealthy, Map.empty, None, None, None, None, false))
      val updateProbeStatus3 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus3))
      updateProbeStatus3.status.health shouldEqual ProbeUnknown

      probe ! ChildMutates(child3, ProbeStatus(timestamp, ProbeKnown, None, ProbeDegraded, Map.empty, None, None, None, None, false))
      val updateProbeStatus4 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus4))
      updateProbeStatus4.status.health shouldEqual ProbeDegraded
    }

    "transition to ProbeSynthetic/ProbeFailed when one child has notified of failed status" in {
      val probeRef = ProbeRef("foo.local:check")
      val probeType = "io.mandelbrot.core.system.AggregateProbe"
      val factory = ProbeBehavior.extensions(probeType).configure(Map.empty)
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val children = Set(child1, child2, child3)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val probe = system.actorOf(Probe.props(probeRef, blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory, children, 0)

      val initializeProbeStatus = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus, Some(status)))

      // probe sets its lifecycle to synthetic
      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))
      updateProbeStatus1.status.lifecycle shouldEqual ProbeSynthetic

      val timestamp = DateTime.now()

      probe ! ChildMutates(child1, ProbeStatus(timestamp, ProbeKnown, None, ProbeHealthy, Map.empty, None, None, None, None, false))
      val updateProbeStatus2 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus2))
      updateProbeStatus2.status.health shouldEqual ProbeUnknown

      probe ! ChildMutates(child2, ProbeStatus(timestamp, ProbeKnown, None, ProbeDegraded, Map.empty, None, None, None, None, false))
      val updateProbeStatus3 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus3))
      updateProbeStatus3.status.health shouldEqual ProbeUnknown

      probe ! ChildMutates(child3, ProbeStatus(timestamp, ProbeKnown, None, ProbeFailed, Map.empty, None, None, None, None, false))
      val updateProbeStatus4 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus4))
      updateProbeStatus4.status.health shouldEqual ProbeFailed
    }

    "notify NotificationService when the alert timeout expires" in {
      val probeRef = ProbeRef("foo.local:check")
      val probeType = "io.mandelbrot.core.system.AggregateProbe"
      val factory = ProbeBehavior.extensions(probeType).configure(Map.empty)
      val policy = CheckPolicy(1.minute, 1.minute, 2.seconds, 1.minute, None)
      val children = Set(child1, child2, child3)
      val notificationService = new TestProbe(_system)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref), notificationService = Some(notificationService.ref)))
      val metricsBus = new MetricsBus()

      val probe = system.actorOf(Probe.props(probeRef, blackhole, services, metricsBus))
      probe ! ChangeProbe(probeType, policy, factory, children, 0)

      val initializeProbeStatus = stateService.expectMsgClass(classOf[InitializeProbeStatus])
      val status = ProbeStatus(DateTime.now(), ProbeInitializing, None, ProbeUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeProbeStatusResult(initializeProbeStatus, Some(status)))

      // probe sets its lifecycle to synthetic
      val updateProbeStatus1 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus1))
      updateProbeStatus1.status.lifecycle shouldEqual ProbeSynthetic

      val timestamp = DateTime.now()

      probe ! ChildMutates(child1, ProbeStatus(timestamp, ProbeKnown, None, ProbeFailed, Map.empty, None, None, None, None, false))
      val updateProbeStatus2 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus2))
      updateProbeStatus2.status.health shouldEqual ProbeUnknown

      probe ! ChildMutates(child2, ProbeStatus(timestamp, ProbeKnown, None, ProbeFailed, Map.empty, None, None, None, None, false))
      val updateProbeStatus3 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus3))
      updateProbeStatus3.status.health shouldEqual ProbeUnknown

      probe ! ChildMutates(child3, ProbeStatus(timestamp, ProbeKnown, None, ProbeFailed, Map.empty, None, None, None, None, false))
      val updateProbeStatus4 = stateService.expectMsgClass(classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus4))
      updateProbeStatus4.status.health shouldEqual ProbeFailed
      notificationService.expectMsgClass(classOf[NotifyHealthChanges])

      // alert timer should fire within 5 seconds
      val updateProbeStatus5 = stateService.expectMsgClass(5.seconds, classOf[UpdateProbeStatus])
      stateService.reply(UpdateProbeStatusResult(updateProbeStatus5))
      val notification = notificationService.expectMsgClass(classOf[NotifyHealthAlerts])
      notification.probeRef shouldEqual probeRef
      notification.health shouldEqual ProbeFailed
      notification.correlation shouldEqual updateProbeStatus4.status.correlation
    }

  }
}
