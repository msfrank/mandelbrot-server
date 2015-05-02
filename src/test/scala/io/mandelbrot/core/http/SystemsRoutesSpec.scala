package io.mandelbrot.core.http

import akka.actor.{PoisonPill, ActorRef}
import akka.pattern.ask
import akka.event.{Logging, LoggingAdapter}
import akka.util.Timeout
import com.typesafe.config.Config
import org.scalatest.{BeforeAndAfter, WordSpec, ShouldMatchers}
import spray.http.StatusCodes
import spray.http.HttpHeaders._
import spray.httpx.SprayJsonSupport._
import spray.testkit.ScalatestRouteTest
import scala.concurrent.Await
import scala.concurrent.duration._

import io.mandelbrot.core.model._
import io.mandelbrot.core.system.RegisterProbeSystem
import io.mandelbrot.core.http.json.JsonProtocol._
import io.mandelbrot.core.{ServerConfig, MandelbrotConfig, ServiceProxy, AkkaConfig}
import io.mandelbrot.core.ConfigConversions._

class SystemsRoutesSpec extends WordSpec with ScalatestRouteTest with ApiService with ShouldMatchers with BeforeAndAfter {

  override def testConfig: Config = AkkaConfig ++ MandelbrotConfig
  override def actorRefFactory = system
  override implicit def log: LoggingAdapter = Logging(system, classOf[SystemsRoutesSpec])

  override val settings: HttpSettings = ServerConfig(system).settings.http
  override implicit val timeout: Timeout = settings.requestTimeout

  var _serviceProxy: ActorRef = ActorRef.noSender
  override def serviceProxy = _serviceProxy

  def withServiceProxy(testCode: => Any) {
    _serviceProxy = system.actorOf(ServiceProxy.props())
    testCode
    _serviceProxy ! PoisonPill
  }

  val policy = CheckPolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
  val properties = Map.empty[String,String]
  val metadata = Map.empty[String,String]
  val probe = CheckSpec("io.mandelbrot.core.system.ScalarProbe", policy, properties, metadata)
  val probes = Map(CheckId("load") -> probe)
  val metrics = Map.empty[CheckId,Map[String,MetricSpec]]

  val agent1 = AgentId("test.1")
  val agent2 = AgentId("test.2")
  val agent3 = AgentId("test.3")
  val agent4 = AgentId("test.4")
  val agent5 = AgentId("test.5")
  val registration1 = AgentRegistration(agent1, "mandelbrot", Map.empty, probes, metrics)
  val registration2 = AgentRegistration(agent2, "mandelbrot", Map.empty, probes, metrics)
  val registration3 = AgentRegistration(agent3, "mandelbrot", Map.empty, probes, metrics)
  val registration4 = AgentRegistration(agent4, "mandelbrot", Map.empty, probes, metrics)
  val registration5 = AgentRegistration(agent5, "mandelbrot", Map.empty, probes, metrics)

  "/v2/systems" should {

    "register a system" in withServiceProxy {
      Post("/v2/systems", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        header("Location") shouldEqual Some(Location("/v2/systems/test.1"))
      }
    }

    "fail to register a system if the system already exists" in withServiceProxy {
      Post("/v2/systems", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        header("Location") shouldEqual Some(Location("/v2/systems/" + registration1.agentId.toString))
      }
      Post("/v2/systems", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.Conflict
      }
    }

    "return a list of systems" in withServiceProxy {
      Await.result(serviceProxy.ask(RegisterProbeSystem(agent1, registration1)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterProbeSystem(agent2, registration2)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterProbeSystem(agent3, registration3)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterProbeSystem(agent4, registration4)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterProbeSystem(agent5, registration5)), 5.seconds)

      Get("/v2/systems") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val page = responseAs[AgentsPage]
        page.agents.map(_.agentId) shouldEqual Vector(agent1, agent2, agent3, agent4, agent5)
        page.last shouldEqual None
      }
    }

    "page through a list of systems" in withServiceProxy {
      Await.result(serviceProxy.ask(RegisterProbeSystem(agent1, registration1)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterProbeSystem(agent2, registration2)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterProbeSystem(agent3, registration3)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterProbeSystem(agent4, registration4)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterProbeSystem(agent5, registration5)), 5.seconds)

      val last = Get("/v2/systems?limit=3") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val page = responseAs[AgentsPage]
        page.agents.map(_.agentId) shouldEqual Vector(agent1, agent2, agent3)
        page.last shouldEqual Some(agent3.toString)
        page.last.get.toString
      }

      Get("/v2/systems?last=%s&limit=3".format(last)) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val page = responseAs[AgentsPage]
        page.agents.map(_.agentId) shouldEqual Vector(agent4, agent5)
        page.last shouldEqual None
      }
    }
  }

  "/v2/systems/(agentId)" should {

    "update a system" in withServiceProxy {
      Post("/v2/systems", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
      Put("/v2/systems/" + registration1.agentId.toString, registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        header("Location") shouldEqual Some(Location("/v2/systems/" + registration1.agentId.toString))
      }
    }

    "fail to update a system if the system doesn't exist" in withServiceProxy {
      Put("/v2/systems/" + agent1.toString, registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "get the registration for a system" in withServiceProxy {
      Post("/v2/systems", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
      Get("/v2/systems/" + registration1.agentId.toString) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val registration = responseAs[AgentRegistration]
        registration shouldEqual registration1
      }
    }

    "fail to get the registration for a system if the system doesn't exist" in withServiceProxy {
      Get("/v2/systems/" + registration1.agentId.toString) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "delete a system" in withServiceProxy {
      Post("/v2/systems", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
      Delete("/v2/systems/" + registration1.agentId.toString) ~> routes ~> check {
        status shouldEqual StatusCodes.Accepted
      }
    }

    "fail to delete a system if the system doesn't exist" in withServiceProxy {
      Delete("/v2/systems/" + registration1.agentId.toString) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }
  }
}
