package io.mandelbrot.core.http

import akka.actor.{PoisonPill, ActorRef}
import akka.pattern.ask
import akka.event.{Logging, LoggingAdapter}
import akka.util.Timeout
import com.typesafe.config.Config
import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.{BeforeAndAfter, WordSpec, ShouldMatchers}
import spray.http.StatusCodes
import spray.httpx.SprayJsonSupport._
import spray.testkit.ScalatestRouteTest
import scala.concurrent.Await
import scala.concurrent.duration._

import io.mandelbrot.core.model._
import io.mandelbrot.core.registry._
import io.mandelbrot.core.model.json.JsonProtocol._
import io.mandelbrot.core.http.v2api.V2Api
import io.mandelbrot.core.{ServerConfig, MandelbrotConfig, ServiceProxy, AkkaConfig}
import io.mandelbrot.core.ConfigConversions._

class GroupsRoutesSpec extends WordSpec with ScalatestRouteTest with V2Api with ShouldMatchers with BeforeAndAfter {

  override def testConfig: Config = AkkaConfig ++ MandelbrotConfig
  override def actorRefFactory = system
  override implicit def log: LoggingAdapter = Logging(system, classOf[GroupsRoutesSpec])

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

  val now = DateTime.now(DateTimeZone.UTC)
  val metadata1 = AgentMetadata(AgentId("agent1"), 1, now, now, None)
  val metadata2 = AgentMetadata(AgentId("agent2"), 1, now, now, None)
  val metadata3 = AgentMetadata(AgentId("agent3"), 1, now, now, None)
  val metadata4 = AgentMetadata(AgentId("agent4"), 1, now, now, None)
  val metadata5 = AgentMetadata(AgentId("agent5"), 1, now, now, None)

  "route /v2/groups/(groupName)" should {

    val groupName = "foobar"

    "return a list of group members" in withServiceProxy {
      Await.result(serviceProxy.ask(AddAgentToGroup(metadata1, groupName)), 5.seconds)
      Await.result(serviceProxy.ask(AddAgentToGroup(metadata2, groupName)), 5.seconds)
      Await.result(serviceProxy.ask(AddAgentToGroup(metadata3, groupName)), 5.seconds)
      Await.result(serviceProxy.ask(AddAgentToGroup(metadata4, groupName)), 5.seconds)
      Await.result(serviceProxy.ask(AddAgentToGroup(metadata5, groupName)), 5.seconds)

      Get(s"/v2/groups/$groupName") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val page = responseAs[MetadataPage]
        page.metadata.length shouldEqual 5
        page.metadata.toSet shouldEqual Set(metadata1, metadata2, metadata3, metadata4, metadata5)
        page.last shouldEqual None
        page.exhausted shouldEqual true
      }
    }

    "page through a list of group members" in withServiceProxy {
      Await.result(serviceProxy.ask(AddAgentToGroup(metadata1, groupName)), 5.seconds)
      Await.result(serviceProxy.ask(AddAgentToGroup(metadata2, groupName)), 5.seconds)
      Await.result(serviceProxy.ask(AddAgentToGroup(metadata3, groupName)), 5.seconds)
      Await.result(serviceProxy.ask(AddAgentToGroup(metadata4, groupName)), 5.seconds)
      Await.result(serviceProxy.ask(AddAgentToGroup(metadata5, groupName)), 5.seconds)

      var members: Vector[AgentMetadata] = Vector.empty

      val last1 = Get(s"/v2/groups/$groupName?limit=2") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val page = responseAs[MetadataPage]
        members = members ++ page.metadata
        page.exhausted shouldEqual false
        page.last.nonEmpty shouldEqual true
        page.last.get.toString
      }

      val last2 = Get(s"/v2/groups/$groupName?last=$last1&limit=2") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val page = responseAs[MetadataPage]
        members = members ++ page.metadata
        page.exhausted shouldEqual false
        page.last.nonEmpty shouldEqual true
        page.last.get.toString
      }

      Get(s"/v2/groups/$groupName?last=$last2&limit=2") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val page = responseAs[MetadataPage]
        members = members ++ page.metadata
        page.exhausted shouldEqual true
        page.last shouldEqual None
      }

      members.length shouldEqual 5
      members.toSet shouldEqual Set(metadata1, metadata2, metadata3, metadata4, metadata5)
    }
  }
}
