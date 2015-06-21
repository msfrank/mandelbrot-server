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
import io.mandelbrot.core.agent.ChangeCheck
import org.joda.time.DateTime
import org.scalatest.ShouldMatchers
import org.scalatest.{WordSpecLike, BeforeAndAfterAll}
import scala.concurrent.duration._

import io.mandelbrot.core.metrics.MetricsBus
import io.mandelbrot.core.state._
import io.mandelbrot.core.model._
import io.mandelbrot.core.{AkkaConfig, Blackhole}
import io.mandelbrot.core.ConfigConversions._

class AggregateCheckSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("AggregateCheckSpec", AkkaConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val child1 = CheckRef("foo.local:check.child1")
  val child2 = CheckRef("foo.local:check.child2")
  val child3 = CheckRef("foo.local:check.child3")
  val blackhole = system.actorOf(Blackhole.props())

  "A Check with aggregate behavior" should {

    "transition to CheckSynthetic/CheckHealthy when all children have notified of healthy status" in {
      val checkRef = CheckRef("foo.local:check")
      val checkType = "io.mandelbrot.core.system.AggregateCheck"
      val factory = CheckBehavior.extensions(checkType).configure(Map.empty)
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val children = Set(child1, child2, child3)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val check = system.actorOf(Check.props(checkRef, blackhole, services, metricsBus))
      check ! ChangeCheck(checkType, policy, factory, children, 0)

      val initializeCheckStatus = stateService.expectMsgClass(classOf[InitializeCheckStatus])
      val status = CheckStatus(DateTime.now(), CheckInitializing, None, CheckUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeCheckStatusResult(initializeCheckStatus, Some(status)))

      // check sets its lifecycle to synthetic
      val updateCheckStatus1 = stateService.expectMsgClass(classOf[UpdateCheckStatus])
      stateService.reply(UpdateCheckStatusResult(updateCheckStatus1))
      updateCheckStatus1.status.lifecycle shouldEqual CheckSynthetic
      updateCheckStatus1.status.health shouldEqual CheckUnknown

      val timestamp = DateTime.now()

      check ! ChildMutates(child1, CheckStatus(timestamp, CheckKnown, None, CheckHealthy, Map.empty, None, None, None, None, false))
      val updateCheckStatus2 = stateService.expectMsgClass(classOf[UpdateCheckStatus])
      stateService.reply(UpdateCheckStatusResult(updateCheckStatus2))
      updateCheckStatus2.status.health shouldEqual CheckUnknown

      check ! ChildMutates(child2, CheckStatus(timestamp, CheckKnown, None, CheckHealthy, Map.empty, None, None, None, None, false))
      val updateCheckStatus3 = stateService.expectMsgClass(classOf[UpdateCheckStatus])
      stateService.reply(UpdateCheckStatusResult(updateCheckStatus3))
      updateCheckStatus3.status.health shouldEqual CheckUnknown

      check ! ChildMutates(child3, CheckStatus(timestamp, CheckKnown, None, CheckHealthy, Map.empty, None, None, None, None, false))
      val updateCheckStatus4 = stateService.expectMsgClass(classOf[UpdateCheckStatus])
      stateService.reply(UpdateCheckStatusResult(updateCheckStatus4))
      updateCheckStatus4.status.health shouldEqual CheckHealthy
    }

    "transition to CheckSynthetic/CheckDegraded when one child has notified of degraded status" in {
      val checkRef = CheckRef("foo.local:check")
      val checkType = "io.mandelbrot.core.system.AggregateCheck"
      val factory = CheckBehavior.extensions(checkType).configure(Map.empty)
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val children = Set(child1, child2, child3)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val check = system.actorOf(Check.props(checkRef, blackhole, services, metricsBus))
      check ! ChangeCheck(checkType, policy, factory, children, 0)

      val initializeCheckStatus = stateService.expectMsgClass(classOf[InitializeCheckStatus])
      val status = CheckStatus(DateTime.now(), CheckInitializing, None, CheckUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeCheckStatusResult(initializeCheckStatus, Some(status)))

      // check sets its lifecycle to synthetic
      val updateCheckStatus1 = stateService.expectMsgClass(classOf[UpdateCheckStatus])
      stateService.reply(UpdateCheckStatusResult(updateCheckStatus1))
      updateCheckStatus1.status.lifecycle shouldEqual CheckSynthetic

      val timestamp = DateTime.now()

      check ! ChildMutates(child1, CheckStatus(timestamp, CheckKnown, None, CheckHealthy, Map.empty, None, None, None, None, false))
      val updateCheckStatus2 = stateService.expectMsgClass(classOf[UpdateCheckStatus])
      stateService.reply(UpdateCheckStatusResult(updateCheckStatus2))
      updateCheckStatus2.status.health shouldEqual CheckUnknown

      check ! ChildMutates(child2, CheckStatus(timestamp, CheckKnown, None, CheckHealthy, Map.empty, None, None, None, None, false))
      val updateCheckStatus3 = stateService.expectMsgClass(classOf[UpdateCheckStatus])
      stateService.reply(UpdateCheckStatusResult(updateCheckStatus3))
      updateCheckStatus3.status.health shouldEqual CheckUnknown

      check ! ChildMutates(child3, CheckStatus(timestamp, CheckKnown, None, CheckDegraded, Map.empty, None, None, None, None, false))
      val updateCheckStatus4 = stateService.expectMsgClass(classOf[UpdateCheckStatus])
      stateService.reply(UpdateCheckStatusResult(updateCheckStatus4))
      updateCheckStatus4.status.health shouldEqual CheckDegraded
    }

    "transition to CheckSynthetic/CheckFailed when one child has notified of failed status" in {
      val checkRef = CheckRef("foo.local:check")
      val checkType = "io.mandelbrot.core.system.AggregateCheck"
      val factory = CheckBehavior.extensions(checkType).configure(Map.empty)
      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
      val children = Set(child1, child2, child3)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
      val metricsBus = new MetricsBus()

      val check = system.actorOf(Check.props(checkRef, blackhole, services, metricsBus))
      check ! ChangeCheck(checkType, policy, factory, children, 0)

      val initializeCheckStatus = stateService.expectMsgClass(classOf[InitializeCheckStatus])
      val status = CheckStatus(DateTime.now(), CheckInitializing, None, CheckUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeCheckStatusResult(initializeCheckStatus, Some(status)))

      // check sets its lifecycle to synthetic
      val updateCheckStatus1 = stateService.expectMsgClass(classOf[UpdateCheckStatus])
      stateService.reply(UpdateCheckStatusResult(updateCheckStatus1))
      updateCheckStatus1.status.lifecycle shouldEqual CheckSynthetic

      val timestamp = DateTime.now()

      check ! ChildMutates(child1, CheckStatus(timestamp, CheckKnown, None, CheckHealthy, Map.empty, None, None, None, None, false))
      val updateCheckStatus2 = stateService.expectMsgClass(classOf[UpdateCheckStatus])
      stateService.reply(UpdateCheckStatusResult(updateCheckStatus2))
      updateCheckStatus2.status.health shouldEqual CheckUnknown

      check ! ChildMutates(child2, CheckStatus(timestamp, CheckKnown, None, CheckDegraded, Map.empty, None, None, None, None, false))
      val updateCheckStatus3 = stateService.expectMsgClass(classOf[UpdateCheckStatus])
      stateService.reply(UpdateCheckStatusResult(updateCheckStatus3))
      updateCheckStatus3.status.health shouldEqual CheckUnknown

      check ! ChildMutates(child3, CheckStatus(timestamp, CheckKnown, None, CheckFailed, Map.empty, None, None, None, None, false))
      val updateCheckStatus4 = stateService.expectMsgClass(classOf[UpdateCheckStatus])
      stateService.reply(UpdateCheckStatusResult(updateCheckStatus4))
      updateCheckStatus4.status.health shouldEqual CheckFailed
    }

    "notify NotificationService when the alert timeout expires" in {
      val checkRef = CheckRef("foo.local:check")
      val checkType = "io.mandelbrot.core.system.AggregateCheck"
      val factory = CheckBehavior.extensions(checkType).configure(Map.empty)
      val policy = CheckPolicy(1.minute, 1.minute, 2.seconds, 1.minute, None)
      val children = Set(child1, child2, child3)
      val notificationService = new TestProbe(_system)
      val stateService = new TestProbe(_system)
      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref), notificationService = Some(notificationService.ref)))
      val metricsBus = new MetricsBus()

      val check = system.actorOf(Check.props(checkRef, blackhole, services, metricsBus))
      check ! ChangeCheck(checkType, policy, factory, children, 0)

      val initializeCheckStatus = stateService.expectMsgClass(classOf[InitializeCheckStatus])
      val status = CheckStatus(DateTime.now(), CheckInitializing, None, CheckUnknown, Map.empty, None, None, None, None, false)
      stateService.reply(InitializeCheckStatusResult(initializeCheckStatus, Some(status)))

      // check sets its lifecycle to synthetic
      val updateCheckStatus1 = stateService.expectMsgClass(classOf[UpdateCheckStatus])
      stateService.reply(UpdateCheckStatusResult(updateCheckStatus1))
      updateCheckStatus1.status.lifecycle shouldEqual CheckSynthetic

      val timestamp = DateTime.now()

      check ! ChildMutates(child1, CheckStatus(timestamp, CheckKnown, None, CheckFailed, Map.empty, None, None, None, None, false))
      val updateCheckStatus2 = stateService.expectMsgClass(classOf[UpdateCheckStatus])
      stateService.reply(UpdateCheckStatusResult(updateCheckStatus2))
      updateCheckStatus2.status.health shouldEqual CheckUnknown

      check ! ChildMutates(child2, CheckStatus(timestamp, CheckKnown, None, CheckFailed, Map.empty, None, None, None, None, false))
      val updateCheckStatus3 = stateService.expectMsgClass(classOf[UpdateCheckStatus])
      stateService.reply(UpdateCheckStatusResult(updateCheckStatus3))
      updateCheckStatus3.status.health shouldEqual CheckUnknown

      check ! ChildMutates(child3, CheckStatus(timestamp, CheckKnown, None, CheckFailed, Map.empty, None, None, None, None, false))
      val updateCheckStatus4 = stateService.expectMsgClass(classOf[UpdateCheckStatus])
      stateService.reply(UpdateCheckStatusResult(updateCheckStatus4))
      updateCheckStatus4.status.health shouldEqual CheckFailed
      notificationService.expectMsgClass(classOf[NotifyHealthChanges])

      // alert timer should fire within 5 seconds
      val updateCheckStatus5 = stateService.expectMsgClass(5.seconds, classOf[UpdateCheckStatus])
      stateService.reply(UpdateCheckStatusResult(updateCheckStatus5))
      val notification = notificationService.expectMsgClass(classOf[NotifyHealthAlerts])
      notification.checkRef shouldEqual checkRef
      notification.health shouldEqual CheckFailed
      notification.correlation shouldEqual updateCheckStatus4.status.correlation
    }

  }
}
