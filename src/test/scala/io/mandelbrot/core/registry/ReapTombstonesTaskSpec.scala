package io.mandelbrot.core.registry

import akka.actor.{PoisonPill, ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.duration._
import org.scalatest.{ShouldMatchers, BeforeAndAfterAll, WordSpecLike}

import io.mandelbrot.core.model._
import io.mandelbrot.core._
import io.mandelbrot.core.ConfigConversions._

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

    "delete an agent registration that is tombstoned" in {

    }
  }
}
