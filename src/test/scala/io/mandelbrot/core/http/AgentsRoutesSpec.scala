package io.mandelbrot.core.http

import akka.actor.{PoisonPill, ActorRef}
import akka.pattern.ask
import akka.event.{Logging, LoggingAdapter}
import akka.util.Timeout
import com.typesafe.config.Config
import io.mandelbrot.core.agent.RegisterAgent
import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.{BeforeAndAfter, WordSpec, ShouldMatchers}
import org.scalatest._
import spray.http.StatusCodes
import spray.http.HttpHeaders._
import spray.httpx.SprayJsonSupport._
import spray.testkit.ScalatestRouteTest
import scala.concurrent.Await
import scala.concurrent.duration._

import io.mandelbrot.core.model._
import io.mandelbrot.core.http.json.JsonProtocol._
import io.mandelbrot.core.{ServerConfig, MandelbrotConfig, ServiceProxy, AkkaConfig}
import io.mandelbrot.core.ConfigConversions._

class AgentsRoutesSpec extends WordSpec with ScalatestRouteTest with V2Api with ShouldMatchers with BeforeAndAfter {

  override def testConfig: Config = AkkaConfig ++ MandelbrotConfig
  override def actorRefFactory = system
  override implicit def log: LoggingAdapter = Logging(system, classOf[AgentsRoutesSpec])

  override val settings: HttpSettings = ServerConfig(system).settings.http
  override implicit val timeout: Timeout = settings.requestTimeout

  var _serviceProxy: ActorRef = ActorRef.noSender
  override def serviceProxy = _serviceProxy

  def withServiceProxy(testCode: => Any) {
    _serviceProxy = system.actorOf(ServiceProxy.props())
    testCode
    _serviceProxy ! PoisonPill
  }

  val checkPolicy = CheckPolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
  val properties = Map.empty[String,String]
  val metadata = Map.empty[String,String]
  val checkId = CheckId("load")
  val checkSpec = CheckSpec("io.mandelbrot.core.check.ScalarCheck", checkPolicy, properties, metadata)
  val checks = Map(checkId -> checkSpec)
  val metrics = Map.empty[CheckId,Map[String,MetricSpec]]
  val agentPolicy = AgentPolicy(5.seconds)

  val agent1 = AgentId("test.1")
  val agent2 = AgentId("test.2")
  val agent3 = AgentId("test.3")
  val agent4 = AgentId("test.4")
  val agent5 = AgentId("test.5")
  val registration1 = AgentSpec(agent1, "mandelbrot", agentPolicy, Map.empty, checks, metrics, Set.empty)
  val registration2 = AgentSpec(agent2, "mandelbrot", agentPolicy, Map.empty, checks, metrics, Set.empty)
  val registration3 = AgentSpec(agent3, "mandelbrot", agentPolicy, Map.empty, checks, metrics, Set.empty)
  val registration4 = AgentSpec(agent4, "mandelbrot", agentPolicy, Map.empty, checks, metrics, Set.empty)
  val registration5 = AgentSpec(agent5, "mandelbrot", agentPolicy, Map.empty, checks, metrics, Set.empty)

  val evaluation = CheckEvaluation(DateTime.now(DateTimeZone.UTC), Some("evaluates healthy"), Some(CheckHealthy), None)

  "route /v2/agents" should {

    "register a check" in withServiceProxy {
      Post("/v2/agents", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        header("Location") shouldEqual Some(Location("/v2/agents/" + registration1.agentId.toString))
      }
    }

    "fail to register a check if the check already exists" in withServiceProxy {
      Post("/v2/agents", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        header("Location") shouldEqual Some(Location("/v2/agents/" + registration1.agentId.toString))
      }
      Post("/v2/agents", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.Conflict
      }
    }

    "return a list of systems" ignore withServiceProxy {
      Await.result(serviceProxy.ask(RegisterAgent(agent1, registration1)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterAgent(agent2, registration2)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterAgent(agent3, registration3)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterAgent(agent4, registration4)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterAgent(agent5, registration5)), 5.seconds)

      Get("/v2/agents") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val page = responseAs[MetadataPage]
        page.metadata.map(_.agentId) shouldEqual Vector(agent1, agent2, agent3, agent4, agent5)
        page.last shouldEqual None
      }
    }

    "page through a list of systems" ignore withServiceProxy {
      Await.result(serviceProxy.ask(RegisterAgent(agent1, registration1)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterAgent(agent2, registration2)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterAgent(agent3, registration3)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterAgent(agent4, registration4)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterAgent(agent5, registration5)), 5.seconds)

      val last = Get("/v2/agents?limit=3") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val page = responseAs[MetadataPage]
        page.metadata.map(_.agentId) shouldEqual Vector(agent1, agent2, agent3)
        page.last shouldEqual Some(agent3.toString)
        page.last.get.toString
      }

      Get("/v2/agents?last=%s&limit=3".format(last)) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val page = responseAs[MetadataPage]
        page.metadata.map(_.agentId) shouldEqual Vector(agent4, agent5)
        page.last shouldEqual None
      }
    }
  }

  "route /v2/agents/(agentId)" should {

    "update a check" in withServiceProxy {
      Post("/v2/agents", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
      Put("/v2/agents/" + registration1.agentId.toString, registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        header("Location") shouldEqual Some(Location("/v2/agents/" + registration1.agentId.toString))
      }
    }

    "fail to update a check if the check doesn't exist" in withServiceProxy {
      Put("/v2/agents/" + agent1.toString, registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "get the registration for a check" in withServiceProxy {
      Post("/v2/agents", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
      Get("/v2/agents/" + registration1.agentId.toString) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val registration = responseAs[AgentSpec]
        registration shouldEqual registration1
      }
    }

    "fail to get the registration for a check if the check doesn't exist" in withServiceProxy {
      Get("/v2/agents/" + registration1.agentId.toString) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "delete a check" in withServiceProxy {
      Post("/v2/agents", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
      Delete("/v2/agents/" + registration1.agentId.toString) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    "fail to delete a check if the check doesn't exist" in withServiceProxy {
      Delete("/v2/agents/" + registration1.agentId.toString) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }
  }

  "route /v2/agents/(agentId)/checks/(checkId)" should {

    "get the status of a check" in withServiceProxy {
      Post("/v2/agents", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        header("Location") shouldEqual Some(Location("/v2/agents/" + registration1.agentId.toString))
      }
      Get("/v2/agents/" + agent1.toString + "/checks/" + checkId.toString) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val checkStatus = responseAs[CheckStatus]
        checkStatus.lifecycle shouldEqual CheckJoining
        checkStatus.health shouldEqual CheckUnknown
      }

    }

    "fail to get the status of a check if the check doesn't exist" in withServiceProxy {
      Get("/v2/agents/" + agent1.toString + "/checks/" + checkId.toString) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }

    }

    "submit an evaluation" in withServiceProxy {
      Post("/v2/agents", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        header("Location") shouldEqual Some(Location("/v2/agents/" + registration1.agentId.toString))
      }
      Post("/v2/agents/" + registration1.agentId.toString + "/checks/" + checkId.toString, evaluation) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
      Get("/v2/agents/" + registration1.agentId.toString + "/checks/" + checkId.toString) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val checkStatus = responseAs[CheckStatus]
        checkStatus.health shouldEqual evaluation.health.get
        checkStatus.summary shouldEqual evaluation.summary
      }
    }

    "fail to submit an evaluation if the check doesn't exist" in withServiceProxy {
      Post("/v2/agents/" + agent1 + "/checks/" + checkId, evaluation) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }
  }

  "route /v2/agents/(agentId)/checks/(checkId)/condition" should {

    "get the latest condition when no timeseries params are specified" in withServiceProxy {
      Post("/v2/agents", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        header("Location") shouldEqual Some(Location("/v2/agents/" + registration1.agentId.toString))
      }
      Post("/v2/agents/" + registration1.agentId.toString + "/checks/" + checkId.toString, evaluation) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
      Get("/v2/agents/" + registration1.agentId.toString + "/checks/" + checkId.toString + "/condition") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val page = responseAs[CheckConditionPage]
        page.history.length shouldEqual 1
        val condition = page.history.head
        condition.health shouldEqual evaluation.health.get
        condition.summary shouldEqual evaluation.summary
      }
    }

    "fail to get the latest condition if the check doesn't exist" in withServiceProxy {
      Post("/v2/agents", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        header("Location") shouldEqual Some(Location("/v2/agents/" + registration1.agentId.toString))
      }
      Post("/v2/agents/" + registration1.agentId.toString + "/checks/" + checkId.toString, evaluation) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
      Get("/v2/agents/" + registration1.agentId.toString + "/checks/" + CheckId("notfound") + "/condition") ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "fail to get the latest condition if the agent doesn't exist" in withServiceProxy {
      Post("/v2/agents", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        header("Location") shouldEqual Some(Location("/v2/agents/" + registration1.agentId.toString))
      }
      Post("/v2/agents/" + registration1.agentId.toString + "/checks/" + checkId.toString, evaluation) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
      Get("/v2/agents/" + AgentId("notfound") + "/checks/" + CheckId("notfound") + "/condition") ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }
  }

  "route /v2/agents/(agentId)/checks/(checkId)/notifications" should {

    "get the latest notifications when no timeseries params are specified" in withServiceProxy {
      Post("/v2/agents", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        header("Location") shouldEqual Some(Location("/v2/agents/" + registration1.agentId.toString))
      }
      Post("/v2/agents/" + registration1.agentId.toString + "/checks/" + checkId.toString, evaluation) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
      Get("/v2/agents/" + registration1.agentId.toString + "/checks/" + checkId.toString + "/notifications") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val page = responseAs[CheckNotificationsPage]
        page.history.length shouldEqual 1
      }
    }

    "fail to get the latest notifications if the check doesn't exist" in withServiceProxy {
      Post("/v2/agents", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        header("Location") shouldEqual Some(Location("/v2/agents/" + registration1.agentId.toString))
      }
      Post("/v2/agents/" + registration1.agentId.toString + "/checks/" + checkId.toString, evaluation) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
      Get("/v2/agents/" + registration1.agentId.toString + "/checks/" + CheckId("notfound") + "/notifications") ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "fail to get the latest notifications if the agent doesn't exist" in withServiceProxy {
      Post("/v2/agents", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        header("Location") shouldEqual Some(Location("/v2/agents/" + registration1.agentId.toString))
      }
      Post("/v2/agents/" + registration1.agentId.toString + "/checks/" + checkId.toString, evaluation) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
      Get("/v2/agents/" + AgentId("notfound") + "/checks/" + CheckId("notfound") + "/notifications") ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }
  }

  "route /v2/agents/(agentId)/checks/(checkId)/metrics" should {

    "get the latest metrics when no timeseries params are specified" in withServiceProxy {
      Post("/v2/agents", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        header("Location") shouldEqual Some(Location("/v2/agents/" + registration1.agentId.toString))
      }
      Post("/v2/agents/" + registration1.agentId.toString + "/checks/" + checkId.toString, evaluation) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
      Get("/v2/agents/" + registration1.agentId.toString + "/checks/" + checkId.toString + "/metrics") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val page = responseAs[CheckMetricsPage]
        page.history.length shouldEqual 1
      }
    }

    "fail to get the latest metrics if the check doesn't exist" in withServiceProxy {
      Post("/v2/agents", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        header("Location") shouldEqual Some(Location("/v2/agents/" + registration1.agentId.toString))
      }
      Post("/v2/agents/" + registration1.agentId.toString + "/checks/" + checkId.toString, evaluation) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
      Get("/v2/agents/" + registration1.agentId.toString + "/checks/" + CheckId("notfound") + "/metrics") ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "fail to get the latest metrics if the agent doesn't exist" in withServiceProxy {
      Post("/v2/agents", registration1) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        header("Location") shouldEqual Some(Location("/v2/agents/" + registration1.agentId.toString))
      }
      Post("/v2/agents/" + registration1.agentId.toString + "/checks/" + checkId.toString, evaluation) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
      Get("/v2/agents/" + AgentId("notfound") + "/checks/" + CheckId("notfound") + "/metrics") ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }
  }
}
