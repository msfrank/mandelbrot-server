package io.mandelbrot.core.http

import akka.actor.{PoisonPill, ActorRef}
import akka.event.{Logging, LoggingAdapter}
import akka.util.Timeout
import com.typesafe.config.Config
import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.{BeforeAndAfter, WordSpec, ShouldMatchers}
import spray.http.StatusCodes
import spray.http.HttpHeaders._
import spray.httpx.SprayJsonSupport._
import spray.testkit.ScalatestRouteTest
import scala.concurrent.duration._

import io.mandelbrot.core.model._
import io.mandelbrot.core.http.v2api.V2Api
import io.mandelbrot.core.model.json.JsonProtocol._
import io.mandelbrot.core.{ServerConfig, MandelbrotConfig, ServiceProxy, AkkaConfig}
import io.mandelbrot.core.ConfigConversions._

class IngestRoutesSpec extends WordSpec with ScalatestRouteTest with V2Api with ShouldMatchers with BeforeAndAfter {

  override def testConfig: Config = AkkaConfig ++ MandelbrotConfig
  override def actorRefFactory = system
  override implicit def log: LoggingAdapter = Logging(system, classOf[AgentsRoutesSpec])

  override val settings: HttpSettings = ServerConfig(system).settings.http
  override implicit val timeout: Timeout = settings.requestTimeout
  implicit val routeTestTimeout = RouteTestTimeout(timeout.duration)

  var _serviceProxy: ActorRef = ActorRef.noSender
  override def serviceProxy = _serviceProxy

  def withServiceProxy(testCode: => Any) {
    _serviceProxy = system.actorOf(ServiceProxy.props())
    testCode
    _serviceProxy ! PoisonPill
  }

  val probeId = ProbeId("load")
  val observation = ScalarMapObservation(probeId, DateTime.now(DateTimeZone.UTC),
    Map("key" -> "value"), Map("load1" -> DataPoint(0, Units, Some(PerMinute))))

  "route /v2/ingest" should {

    "submit an observation" in withServiceProxy {
//      Post("/v2/ingest", observation) ~> routes ~> check {
//        status shouldEqual StatusCodes.OK
//      }
    }
  }
}
