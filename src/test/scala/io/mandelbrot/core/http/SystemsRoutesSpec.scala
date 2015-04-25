package io.mandelbrot.core.http

import akka.actor.ActorRef
import akka.pattern.ask
import akka.event.{Logging, LoggingAdapter}
import akka.util.Timeout
import com.typesafe.config.Config
import io.mandelbrot.core.system.RegisterProbeSystem
import org.scalatest.{WordSpec, ShouldMatchers}
import spray.http.StatusCodes
import spray.http.HttpHeaders._
import spray.httpx.SprayJsonSupport._
import spray.testkit.ScalatestRouteTest
import scala.concurrent.Await
import scala.concurrent.duration._
import java.net.URI

import io.mandelbrot.core.model._
import io.mandelbrot.core.http.json.JsonProtocol._
import io.mandelbrot.core.{ServerConfig, MandelbrotConfig, ServiceProxy, AkkaConfig}
import io.mandelbrot.core.ConfigConversions._

class SystemsRoutesSpec extends WordSpec with ScalatestRouteTest with ApiService with ShouldMatchers {

  override def testConfig: Config = AkkaConfig ++ MandelbrotConfig
  override def actorRefFactory = system
  override implicit def log: LoggingAdapter = Logging(system, classOf[SystemsRoutesSpec])

  override val settings: HttpSettings = ServerConfig(system).settings.http
  override implicit val timeout: Timeout = settings.requestTimeout

  override implicit val serviceProxy: ActorRef = system.actorOf(ServiceProxy.props(), "service-proxy")

  "/v2/systems" should {

    val policy = CheckPolicy(5.seconds, 5.seconds, 5.seconds, 5.seconds, None)
    val properties = Map.empty[String,String]
    val metadata = Map.empty[String,String]
    val children = Map.empty[String,CheckSpec]
    val probe = CheckSpec("io.mandelbrot.core.system.ScalarProbe", policy, properties, metadata, children)
    val probes = Map("load" -> probe)
    val metrics = Map.empty[MetricSource,MetricSpec]
    val registration = AgentRegistration(Resource("agent"), "mandelbrot", Map.empty, probes, metrics)

    "register a system" in {
      val op = RegisterProbeSystem(new URI("test:1"), registration)
      Post("/v2/systems", op) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        header("Location") shouldEqual Some(Location("/v2/systems/test:1"))
      }
    }

    "return a list of systems" in {
      val uri1 = new URI("test:1")
      val uri2 = new URI("test:2")
      val uri3 = new URI("test:3")
      val uri4 = new URI("test:4")
      val uri5 = new URI("test:5")
      Await.result(serviceProxy.ask(RegisterProbeSystem(uri1, registration)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterProbeSystem(uri2, registration)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterProbeSystem(uri3, registration)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterProbeSystem(uri4, registration)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterProbeSystem(uri5, registration)), 5.seconds)

      Get("/v2/systems") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val page = responseAs[AgentsPage]
        page.systems.map(_.uri) shouldEqual Vector(uri1, uri2, uri3, uri4, uri5)
        page.last shouldEqual None
      }
    }

    "page through a list of systems" in {
      val uri1 = new URI("test:1")
      val uri2 = new URI("test:2")
      val uri3 = new URI("test:3")
      val uri4 = new URI("test:4")
      val uri5 = new URI("test:5")
      Await.result(serviceProxy.ask(RegisterProbeSystem(uri1, registration)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterProbeSystem(uri2, registration)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterProbeSystem(uri3, registration)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterProbeSystem(uri4, registration)), 5.seconds)
      Await.result(serviceProxy.ask(RegisterProbeSystem(uri5, registration)), 5.seconds)

      val last = Get("/v2/systems?limit=3") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val page = responseAs[AgentsPage]
        page.systems.map(_.uri) shouldEqual Vector(uri1, uri2, uri3)
        page.last shouldEqual Some(uri3.toString)
        page.last.get.toString
      }

      Get("/v2/systems?last=%s&limit=3".format(last)) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val page = responseAs[AgentsPage]
        page.systems.map(_.uri) shouldEqual Vector(uri4, uri5)
        page.last shouldEqual None
      }
    }
  }
}
