package io.mandelbrot.core.registry

import akka.actor.{PoisonPill, ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import org.joda.time.{DateTimeZone, DateTime}
import spray.json.{JsObject, JsString}
import scala.concurrent.duration._
import org.scalatest.{ShouldMatchers, BeforeAndAfterAll, WordSpecLike}
import org.scalatest.LoneElement._

import io.mandelbrot.core.model._
import io.mandelbrot.core.agent._
import io.mandelbrot.core._
import io.mandelbrot.core.ConfigConversions._
import io.mandelbrot.core.model.Conversions._

class ReapTombstonesTaskSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ReapTombstonesTaskSpec", AkkaConfig ++ MandelbrotConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val settings = ServerConfig(system).settings

  def withServiceProxy(testCode: (ActorRef) => Any) {
    val services = system.actorOf(ServiceProxy.props())
    testCode(services)
    services ! PoisonPill
  }

  "A ReapTombstonesTask" should {

    "do nothing when there are no agent registration tombstones" in withServiceProxy { serviceProxy =>
      val olderThan = DateTime.now(DateTimeZone.UTC)
      val timeout = 5.seconds
      val task = system.actorOf(ReapTombstonesTask.props(olderThan, timeout, 100, serviceProxy, self))
      val reaperComplete = expectMsgClass(timeout + 2.seconds, classOf[ReaperComplete])
      reaperComplete.seen shouldEqual 0
      reaperComplete.deleted shouldEqual 0
    }

    "delete an agent registration that is tombstoned" in withServiceProxy { serviceProxy =>

      val agentId = AgentId("foo")
      val probePolicy = ProbePolicy(PerMinute)
      val probes = Map(ProbeId("load") -> ProbeSpec(probePolicy, Map("load1" -> MetricSpec(GaugeSource, Units))))
      val checkPolicy = CheckPolicy(5.seconds, 5.seconds, 5.seconds, 0.seconds, None)
      val check = CheckSpec("io.mandelbrot.core.check.TimeseriesCheck", checkPolicy,
        Some(JsObject("evaluation" -> JsString("probe:system.load:load1:p99:1minute:host=foo.com > 1"))), Map.empty)
      val checks = Map(CheckId("load") -> check)
      val agentPolicy = AgentPolicy(0.seconds)
      val registration = AgentSpec(agentId, "mandelbrot", agentPolicy, probes, checks)

      serviceProxy ! RegisterAgent(agentId, registration)
      val registerAgentResult = expectMsgClass(10.seconds, classOf[RegisterAgentResult])

      val probeRef = ProbeRef(agentId, ProbeId("load"))
      val timestamp = Timestamp()
      val observation = ScalarMapObservation(probeRef.probeId, timestamp, Map.empty, Map.empty)
      serviceProxy ! ProcessProbeObservation(probeRef, observation)
      expectMsgClass(10.seconds, classOf[ProcessProbeObservationResult])

      serviceProxy ! RetireAgent(agentId)
      expectMsgClass(classOf[RetireAgentResult])

      val olderThan = DateTime.now(DateTimeZone.UTC).plus(5000)
      serviceProxy ! ListTombstones(olderThan, limit = 100)
      val listTombstonesResult = expectMsgClass(classOf[ListTombstonesResult])
      val tombstone = listTombstonesResult.tombstones.loneElement
      tombstone.agentId shouldEqual agentId
      tombstone.generation shouldEqual 1

      val timeout = 5.seconds
      val task = system.actorOf(ReapTombstonesTask.props(olderThan, timeout, 100, serviceProxy, self))
      val reaperComplete = expectMsgClass(timeout + 10.seconds, classOf[ReaperComplete])
      reaperComplete.seen shouldEqual 1
      reaperComplete.deleted shouldEqual 1
    }
  }
}
