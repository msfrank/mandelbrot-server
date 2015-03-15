package io.mandelbrot.core.http

import akka.actor.ActorRef
import akka.event.{Logging, LoggingAdapter}
import akka.util.Timeout
import com.typesafe.config.Config
import org.scalatest.{WordSpec, ShouldMatchers}
import spray.http.StatusCodes
import spray.testkit.ScalatestRouteTest
import scala.concurrent.duration._

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

    "return a list of systems" in {
      Get("/v2/systems") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
    }
  }
}
