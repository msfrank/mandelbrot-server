package io.mandelbrot.core.registry

import akka.actor.{PoisonPill, ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.{ShouldMatchers, BeforeAndAfterAll, WordSpecLike}
import scala.concurrent.duration._

import io.mandelbrot.core.model._
import io.mandelbrot.core._
import io.mandelbrot.core.ConfigConversions._

class RegistryManagerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("RegistryManagerSpec", AkkaConfig ++ MandelbrotConfig))

  // shutdown the actor system
  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val settings = ServerConfig(system).settings

  def withRegistryService(testCode: (ActorRef) => Any) {
    val registryService = system.actorOf(RegistryManager.props(settings.registry))
    testCode(registryService)
    registryService ! PoisonPill
  }

  val policy = CheckPolicy(joiningTimeout = 1.minute, checkTimeout = 1.minute,
    alertTimeout = 1.minute, leavingTimeout = 1.minute, notifications = None)

  val agent1 = AgentId("test.registry.manager.1")
  val checks1 = Map(CheckId("check1") -> CheckSpec("check.type.test", policy, Map.empty, Map.empty))
  val metrics1 = Map(CheckId("check1") -> Map("metric1" -> MetricSpec(GaugeSource, Units, None, None, None)))
  val registration1 = AgentRegistration(agent1, "mandelbrot", Map.empty, checks1, metrics1)

  val agent2 = AgentId("test.registry.manager.2")
  val checks2 = Map(CheckId("check2") -> CheckSpec("check.type.test", policy, Map.empty, Map.empty))
  val metrics2 = Map(CheckId("check2") -> Map("metric2" -> MetricSpec(GaugeSource, Units, None, None, None)))
  val registration2 = AgentRegistration(agent2, "mandelbrot", Map.empty, checks2, metrics2)

  val agent3 = AgentId("test.registry.manager.3")
  val checks3 = Map(CheckId("check3") -> CheckSpec("check.type.test", policy, Map.empty, Map.empty))
  val metrics3 = Map(CheckId("check3") -> Map("metric3" -> MetricSpec(GaugeSource, Units, None, None, None)))
  val registration3 = AgentRegistration(agent3, "mandelbrot", Map.empty, checks3, metrics3)

  val agent4 = AgentId("test.registry.manager.4")
  val checks4 = Map(CheckId("check4") -> CheckSpec("check.type.test", policy, Map.empty, Map.empty))
  val metrics4 = Map(CheckId("check4") -> Map("metric4" -> MetricSpec(GaugeSource, Units, None, None, None)))
  val registration4 = AgentRegistration(agent4, "mandelbrot", Map.empty, checks4, metrics4)

  val agent5 = AgentId("test.registry.manager.5")
  val checks5 = Map(CheckId("check5") -> CheckSpec("check.type.test", policy, Map.empty, Map.empty))
  val metrics5 = Map(CheckId("check5") -> Map("metric5" -> MetricSpec(GaugeSource, Units, None, None, None)))
  val registration5 = AgentRegistration(agent5, "mandelbrot", Map.empty, checks5, metrics5)

  def withTestData(testCode: (ActorRef) => Any): Unit = {
    withRegistryService { registryService =>
      testCode(registryService)
    }
  }

  "A RegistryManager" when {

    "servicing a CreateRegistration request" should {

      "create a registration if the agent doesn't exist" in withRegistryService { registryService =>
        registryService ! CreateRegistration(agent1, registration1)
        val createRegistrationResult = expectMsgClass(classOf[CreateRegistrationResult])
        createRegistrationResult.metadata.agentId shouldEqual agent1
        createRegistrationResult.metadata.lsn shouldEqual 1
      }

      "overwrite a registration if the agent already exists" in withRegistryService { registryService =>
        registryService ! CreateRegistration(agent1, registration1)
        val createRegistrationResult1 = expectMsgClass(classOf[CreateRegistrationResult])
        createRegistrationResult1.metadata.agentId shouldEqual agent1
        createRegistrationResult1.metadata.lsn shouldEqual 1

        registryService ! CreateRegistration(agent1, registration1)
        val createRegistrationResult2 = expectMsgClass(classOf[CreateRegistrationResult])
        createRegistrationResult2.metadata.agentId shouldEqual agent1
        createRegistrationResult2.metadata.lsn shouldEqual 1
      }
    }

    "servicing an UpdateRegistration request" should {

      "update a registration if the agent exists" in withRegistryService { registryService =>
        registryService ! CreateRegistration(agent1, registration1)
        val createRegistrationResult = expectMsgClass(classOf[CreateRegistrationResult])
        createRegistrationResult.metadata.agentId shouldEqual agent1
        createRegistrationResult.metadata.lsn shouldEqual 1

        val updatedRegistration = registration1.copy(metadata = Map("foo" -> "bar"))
        val updatedMetadata = createRegistrationResult.metadata.copy(lsn = 2)
        registryService ! UpdateRegistration(agent1, updatedRegistration, updatedMetadata)
        val updateRegistrationResult = expectMsgClass(classOf[UpdateRegistrationResult])

        registryService ! GetRegistration(agent1)
        val getRegistrationResult = expectMsgClass(classOf[GetRegistrationResult])
        getRegistrationResult.registration shouldEqual updatedRegistration
        getRegistrationResult.metadata shouldEqual updatedMetadata
      }

      "create a registration if the agent doesn't exist" in withRegistryService { registryService =>
        val timestamp = DateTime.now(DateTimeZone.UTC)
        val metadata = AgentMetadata(agent1, joinedOn = timestamp, lastUpdate = timestamp, lsn = 2)
        registryService ! UpdateRegistration(agent1, registration1, metadata)
        val updateRegistrationResult = expectMsgClass(classOf[UpdateRegistrationResult])

        registryService ! GetRegistration(agent1)
        val getRegistrationResult = expectMsgClass(classOf[GetRegistrationResult])
        getRegistrationResult.registration shouldEqual registration1
        getRegistrationResult.metadata shouldEqual metadata
      }
    }

    "servicing a DeleteRegistration request" should {

      "delete a registration if the agent exists" in withRegistryService { registryService =>
        registryService ! CreateRegistration(agent1, registration1)
        val createRegistrationResult = expectMsgClass(classOf[CreateRegistrationResult])
        createRegistrationResult.metadata.agentId shouldEqual agent1
        createRegistrationResult.metadata.lsn shouldEqual 1

        registryService ! DeleteRegistration(agent1)
        val deleteRegistrationResult = expectMsgClass(classOf[DeleteRegistrationResult])
      }

      "do nothing if the agent doesn't exist" in withRegistryService { registryService =>
        registryService ! DeleteRegistration(agent1)
        val deleteRegistrationResult = expectMsgClass(classOf[DeleteRegistrationResult])
      }
    }

    "servicing a GetRegistration request" should {

    }

    "servicing a ListRegistrations request" should {

    }
  }
}
