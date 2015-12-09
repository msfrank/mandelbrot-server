package io.mandelbrot.core.check

import akka.actor.{ActorRef, PoisonPill, ActorSystem}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import org.scalatest.ShouldMatchers
import org.scalatest.{WordSpecLike, BeforeAndAfterAll}
import scala.concurrent.duration._

import io.mandelbrot.core.model._
import io.mandelbrot.core._
import io.mandelbrot.core.ConfigConversions._

class CheckSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("CheckSpec", AkkaConfig ++ MandelbrotConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  def withServiceProxy(testCode: (ActorRef) => Any) {
    val services = system.actorOf(ServiceProxy.props())
    testCode(services)
    services ! PoisonPill
  }

  val checkRef = CheckRef("foo.local:check")
  val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
  val checkType = "io.mandelbrot.core.check.TestCheck"
  val generation = 1L
  val blackhole = system.actorOf(Blackhole.props())

  "A Check" should {

    "have an initial state" in {
      val services = system.actorOf(TestServiceProxy.props())
      val check = TestActorRef(new Check(checkRef, generation, blackhole, services))
      val settings = TestProcessorSettings(Vector.empty, 1.minute)
      val props = TestProcessor.props(settings)
      check ! ChangeCheck(checkType, policy, props, Set.empty, 0)
      val underlying = check.underlyingActor
      underlying.lifecycle shouldEqual CheckInitializing
      underlying.health shouldEqual CheckUnknown
      underlying.summary shouldEqual None
      underlying.lastChange shouldEqual None
      underlying.lastUpdate shouldEqual None
      underlying.correlationId shouldEqual None
      underlying.acknowledgementId shouldEqual None
      underlying.squelch shouldEqual false
    }

    "transition from initializing to known after the processor publishes the first status" in {
      val services = system.actorOf(ServiceProxy.props())
      val check = system.actorOf(Check.props(checkRef, generation, blackhole, services))
      val settings = TestProcessorSettings(Vector((CheckHealthy,Some("summary"))), 3.seconds)
      val props = TestProcessor.props(settings)

      check ! ChangeCheck(checkType, policy, props, Set.empty, 0)

      check ! GetCheckStatus(checkRef)
      val getCheckStatusResult1 = expectMsgClass(classOf[GetCheckStatusResult])
      getCheckStatusResult1.status.lifecycle shouldEqual CheckInitializing
      getCheckStatusResult1.status.health shouldEqual CheckUnknown
      getCheckStatusResult1.status.summary shouldEqual None
      getCheckStatusResult1.status.correlation shouldEqual None
      getCheckStatusResult1.status.acknowledged shouldEqual None
      getCheckStatusResult1.status.squelched shouldEqual false

      // wait for 5 seconds, to allow test check to emit status
      expectNoMsg(5.seconds)

      check ! GetCheckStatus(checkRef)
      val getCheckStatusResult2 = expectMsgClass(classOf[GetCheckStatusResult])
      getCheckStatusResult2.status.lifecycle shouldEqual CheckKnown
      getCheckStatusResult2.status.health shouldEqual CheckHealthy
      getCheckStatusResult2.status.summary shouldEqual Some("summary")
    }

//    "update behavior" in {
//      val checkRef = CheckRef("foo.local:check")
//      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
//      val checkType = "io.mandelbrot.core.check.TestBehavior"
//      val stateService = new TestProbe(_system)
//      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
//      val metricsBus = new ObservationBus()
//
//      val factory1 = CheckBehavior.extensions(checkType).configure(Map("key" -> "value1"))
//
//      val check = TestActorRef(new Check(checkRef, generation, blackhole, services, metricsBus))
//      check ! ChangeCheck(checkType, policy, factory1, Set.empty, 0)
//
//      val getStatus1 = stateService.expectMsgClass(classOf[GetStatus])
//      val status1 = CheckStatus(generation, DateTime.now(), CheckKnown, None, CheckHealthy,
//        Map.empty, None, None, None, None, false)
//      stateService.reply(GetStatusResult(getStatus1, Some(status1)))
//
//      val updateCheckStatus1 = stateService.expectMsgClass(classOf[UpdateStatus])
//      stateService.reply(UpdateStatusResult(updateCheckStatus1))
//
//      check.underlyingActor.processor shouldBe a [TestProcessor]
//      check.underlyingActor.processor should have ('properties (Map("key" -> "value1")))
//
//      val factory2 = CheckBehavior.extensions(checkType).configure(Map("key" -> "value2"))
//      check ! ChangeCheck(checkType, policy, factory2, Set.empty, 1)
//
//      val getStatus2 = stateService.expectMsgClass(classOf[GetStatus])
//      val status2 = CheckStatus(generation, DateTime.now(), CheckKnown, None, CheckHealthy,
//        Map.empty, None, None, None, None, false)
//      stateService.reply(GetStatusResult(getStatus2, Some(status2)))
//
//      val updateCheckStatus2 = stateService.expectMsgClass(classOf[UpdateStatus])
//      stateService.reply(UpdateStatusResult(updateCheckStatus2))
//
//      check.underlyingActor.processor shouldBe a [TestProcessor]
//      check.underlyingActor.processor should have ('properties (Map("key" -> "value2")))
//    }
//
//    "change behaviors" in {
//      val checkRef = CheckRef("foo.local:check")
//      val child1 = CheckRef("foo.local:check.child1")
//      val child2 = CheckRef("foo.local:check.child2")
//      val child3 = CheckRef("foo.local:check.child3")
//      val children = Set(child1, child2, child3)
//      val stateService = new TestProbe(_system)
//      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
//      val metricsBus = new ObservationBus()
//      val policy = CheckPolicy(1.minute, 2.seconds, 1.minute, 1.minute, None)
//      val checkType1 = "io.mandelbrot.core.check.TestBehavior"
//      val factory1 = CheckBehavior.extensions(checkType1).configure(Map.empty)
//
//      val check = TestActorRef(new Check(checkRef, generation, blackhole, services, metricsBus))
//      check ! ChangeCheck(checkType1, policy, factory1, children, 0)
//
//      val getStatus1 = stateService.expectMsgClass(classOf[GetStatus])
//      val status1 = CheckStatus(generation, DateTime.now(), CheckKnown, None, CheckHealthy,
//        Map.empty, None, None, None, None, false)
//      stateService.reply(GetStatusResult(getStatus1, Some(status1)))
//
//      val updateCheckStatus1 = stateService.expectMsgClass(classOf[UpdateStatus])
//      stateService.reply(UpdateStatusResult(updateCheckStatus1))
//
//      val checkType2 = "io.mandelbrot.core.check.TestChangeBehavior"
//      val factory2 = CheckBehavior.extensions(checkType2).configure(Map.empty)
//      check ! ChangeCheck(checkType2, policy, factory2, children, 1)
//
//      val getStatus2 = stateService.expectMsgClass(classOf[GetStatus])
//      val status2 = CheckStatus(generation, DateTime.now(), CheckKnown, None, CheckHealthy,
//        Map.empty, None, None, None, None, false)
//      stateService.reply(GetStatusResult(getStatus2, Some(status2)))
//
//      val updateCheckStatus2 = stateService.expectMsgClass(classOf[UpdateStatus])
//      stateService.reply(UpdateStatusResult(updateCheckStatus2))
//
//      check.underlyingActor.children shouldEqual children
//      check.underlyingActor.policy shouldEqual policy
//      check.underlyingActor.processor shouldBe a [TestProcessorChange]
//    }
//
//    "transition to retired behavior" in {
//      val checkRef = CheckRef("foo.local:check")
//      val policy = CheckPolicy(1.minute, 1.minute, 1.minute, 1.minute, None)
//      val checkType = "io.mandelbrot.core.check.TestBehavior"
//      val factory = CheckBehavior.extensions(checkType).configure(Map.empty)
//      val stateService = new TestProbe(_system)
//      val services = system.actorOf(TestServiceProxy.props(stateService = Some(stateService.ref)))
//      val metricsBus = new ObservationBus()
//
//      val check = system.actorOf(Check.props(checkRef, generation, blackhole, services, metricsBus))
//      watch(check)
//      check ! ChangeCheck(checkType, policy, factory, Set.empty, lsn = 1)
//
//      val getStatus1 = stateService.expectMsgClass(classOf[GetStatus])
//      val status1 = CheckStatus(generation, DateTime.now(), CheckKnown, None, CheckHealthy,
//        Map.empty, None, None, None, None, false)
//      stateService.reply(GetStatusResult(getStatus1, Some(status1)))
//
//      val updateCheckStatus1 = stateService.expectMsgClass(classOf[UpdateStatus])
//      stateService.reply(UpdateStatusResult(updateCheckStatus1))
//
//      check ! RetireCheck(lsn = 2)
//      check ! PoisonPill
//
//      val updateCheckStatus2 = stateService.expectMsgClass(classOf[UpdateStatus])
//      stateService.reply(UpdateStatusResult(updateCheckStatus2))
//
//      val result = expectMsgClass(classOf[Terminated])
//      result.actor shouldEqual check
//    }
  }
}
