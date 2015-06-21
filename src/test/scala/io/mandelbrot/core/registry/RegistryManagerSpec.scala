package io.mandelbrot.core.registry

import akka.actor.{PoisonPill, ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest.{ShouldMatchers, BeforeAndAfterAll, WordSpecLike}
import org.scalatest.LoneElement._
import org.scalatest.Inside._
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
  val registration1 = AgentSpec(agent1, "mandelbrot", Map.empty, checks1, metrics1, Set.empty)

  val agent2 = AgentId("test.registry.manager.2")
  val checks2 = Map(CheckId("check2") -> CheckSpec("check.type.test", policy, Map.empty, Map.empty))
  val metrics2 = Map(CheckId("check2") -> Map("metric2" -> MetricSpec(GaugeSource, Units, None, None, None)))
  val registration2 = AgentSpec(agent2, "mandelbrot", Map.empty, checks2, metrics2, Set.empty)

  val agent3 = AgentId("test.registry.manager.3")
  val checks3 = Map(CheckId("check3") -> CheckSpec("check.type.test", policy, Map.empty, Map.empty))
  val metrics3 = Map(CheckId("check3") -> Map("metric3" -> MetricSpec(GaugeSource, Units, None, None, None)))
  val registration3 = AgentSpec(agent3, "mandelbrot", Map.empty, checks3, metrics3, Set.empty)

  val agent4 = AgentId("test.registry.manager.4")
  val checks4 = Map(CheckId("check4") -> CheckSpec("check.type.test", policy, Map.empty, Map.empty))
  val metrics4 = Map(CheckId("check4") -> Map("metric4" -> MetricSpec(GaugeSource, Units, None, None, None)))
  val registration4 = AgentSpec(agent4, "mandelbrot", Map.empty, checks4, metrics4, Set.empty)

  val agent5 = AgentId("test.registry.manager.5")
  val checks5 = Map(CheckId("check5") -> CheckSpec("check.type.test", policy, Map.empty, Map.empty))
  val metrics5 = Map(CheckId("check5") -> Map("metric5" -> MetricSpec(GaugeSource, Units, None, None, None)))
  val registration5 = AgentSpec(agent5, "mandelbrot", Map.empty, checks5, metrics5, Set.empty)

  val joinedOn = new DateTime(0, DateTimeZone.UTC)
  val registrationHistory1 = AgentSpec(agent1, "mandelbrot", Map("history" -> "1"), checks1, metrics1, Set.empty)
  val metadataHistory1 = AgentMetadata(agent1, 1, joinedOn, joinedOn, None)
  val registrationHistory2 = AgentSpec(agent1, "mandelbrot", Map("history" -> "2"), checks1, metrics1, Set.empty)
  val metadataHistory2 = AgentMetadata(agent1, 1, joinedOn, joinedOn.plusMinutes(1), None)
  val registrationHistory3 = AgentSpec(agent1, "mandelbrot", Map("history" -> "3"), checks1, metrics1, Set.empty)
  val metadataHistory3 = AgentMetadata(agent1, 1, joinedOn, joinedOn.plusMinutes(1), None)
  val registrationHistory4 = AgentSpec(agent1, "mandelbrot", Map("history" -> "4"), checks1, metrics1, Set.empty)
  val metadataHistory4 = AgentMetadata(agent1, 1, joinedOn, joinedOn.plusMinutes(1), None)
  val registrationHistory5 = AgentSpec(agent1, "mandelbrot", Map("history" -> "5"), checks1, metrics1, Set.empty)
  val metadataHistory5 = AgentMetadata(agent1, 1, joinedOn, joinedOn.plusMinutes(1), None)

  def withTestData(testCode: (ActorRef) => Any): Unit = {
    withRegistryService { registryService =>

      registryService ! PutRegistration(agent1, registrationHistory1, metadataHistory1, lsn = 1)
      expectMsgClass(classOf[PutRegistrationResult])

      registryService ! CommitRegistration(agent1, registrationHistory2, metadataHistory2, lsn = 2)
      expectMsgClass(classOf[CommitRegistrationResult])

      registryService ! CommitRegistration(agent1, registrationHistory3, metadataHistory3, lsn = 3)
      expectMsgClass(classOf[CommitRegistrationResult])

      registryService ! CommitRegistration(agent1, registrationHistory4, metadataHistory4, lsn = 4)
      expectMsgClass(classOf[CommitRegistrationResult])

      registryService ! CommitRegistration(agent1, registrationHistory5, metadataHistory5, lsn = 5)
      expectMsgClass(classOf[CommitRegistrationResult])

      testCode(registryService)
    }
  }

  "A RegistryManager" when {

    "servicing a PutRegistration request" should {

      "create a registration if the agent doesn't exist" in withRegistryService { registryService =>
        val timestamp = DateTime.now(DateTimeZone.UTC)
        val metadata = AgentMetadata(agent1, 1, timestamp, timestamp, None)
        registryService ! PutRegistration(agent1, registration1, metadata, lsn = 1)
        val createRegistrationResult = expectMsgClass(classOf[PutRegistrationResult])
        createRegistrationResult.metadata shouldEqual metadata
        registryService ! GetRegistration(agent1)
        val getRegistrationResult = expectMsgClass(classOf[GetRegistrationResult])
        getRegistrationResult.registration shouldEqual registration1
        getRegistrationResult.metadata shouldEqual metadata
        getRegistrationResult.lsn shouldEqual 1
      }

      "overwrite a registration if the agent already exists" in withRegistryService { registryService =>
        val timestamp = DateTime.now(DateTimeZone.UTC)
        val metadata = AgentMetadata(agent1, 1, timestamp, timestamp, None)
        registryService ! PutRegistration(agent1, registration1, metadata, lsn = 1)
        val createRegistrationResult1 = expectMsgClass(classOf[PutRegistrationResult])
        createRegistrationResult1.metadata shouldEqual metadata

        registryService ! PutRegistration(agent1, registration1, metadata, lsn = 2)
        val createRegistrationResult2 = expectMsgClass(classOf[PutRegistrationResult])
        createRegistrationResult2.metadata shouldEqual metadata

        registryService ! GetRegistration(agent1)
        val getRegistrationResult = expectMsgClass(classOf[GetRegistrationResult])
        getRegistrationResult.registration shouldEqual registration1
        getRegistrationResult.metadata shouldEqual metadata
        getRegistrationResult.lsn shouldEqual 2
      }
    }

    "servicing an CommitRegistration request" should {

      "update a registration if the agent exists" in withRegistryService { registryService =>
        val timestamp1 = DateTime.now(DateTimeZone.UTC)
        val metadata1 = AgentMetadata(agent1, 1, timestamp1, timestamp1, None)
        registryService ! PutRegistration(agent1, registration1, metadata1, lsn = 1)
        val createRegistrationResult = expectMsgClass(classOf[PutRegistrationResult])
        createRegistrationResult.metadata shouldEqual metadata1

        val updatedRegistration = registration1.copy(metadata = Map("foo" -> "bar"))
        val updatedMetadata = metadata1.copy(lastUpdate = DateTime.now(DateTimeZone.UTC))
        registryService ! CommitRegistration(agent1, updatedRegistration, updatedMetadata, lsn = 2)
        val updateRegistrationResult = expectMsgClass(classOf[CommitRegistrationResult])

        registryService ! GetRegistration(agent1)
        val getRegistrationResult = expectMsgClass(classOf[GetRegistrationResult])
        getRegistrationResult.registration shouldEqual updatedRegistration
        getRegistrationResult.metadata shouldEqual updatedMetadata
        getRegistrationResult.lsn shouldEqual 2
      }

      "create a registration if the agent doesn't exist" in withRegistryService { registryService =>
        val timestamp = DateTime.now(DateTimeZone.UTC)
        val metadata = AgentMetadata(agent1, 1, timestamp, timestamp, None)
        registryService ! CommitRegistration(agent1, registration1, metadata, lsn = 1)
        val updateRegistrationResult = expectMsgClass(classOf[CommitRegistrationResult])

        registryService ! GetRegistration(agent1)
        val getRegistrationResult = expectMsgClass(classOf[GetRegistrationResult])
        getRegistrationResult.registration shouldEqual registration1
        getRegistrationResult.metadata shouldEqual metadata
        getRegistrationResult.lsn shouldEqual 1
      }
    }

    "servicing a DeleteRegistration request" should {

      "delete a registration if the agent exists" in withRegistryService { registryService =>
        val timestamp = DateTime.now(DateTimeZone.UTC)
        val metadata = AgentMetadata(agent1, 1, timestamp, timestamp, None)
        registryService ! PutRegistration(agent1, registration1, metadata, lsn = 1)
        val createRegistrationResult = expectMsgClass(classOf[PutRegistrationResult])
        createRegistrationResult.metadata shouldEqual metadata

        registryService ! DeleteRegistration(agent1, generation = metadata.generation)
        val deleteRegistrationResult = expectMsgClass(classOf[DeleteRegistrationResult])

        registryService ! GetRegistration(agent1)
        val getRegistrationResult = expectMsgClass(classOf[RegistryServiceOperationFailed])
        getRegistrationResult.failure shouldEqual ApiException(ResourceNotFound)
      }

      "do nothing if the agent doesn't exist" in withRegistryService { registryService =>
        registryService ! DeleteRegistration(agent1, generation = 1)
        val deleteRegistrationResult = expectMsgClass(classOf[DeleteRegistrationResult])

        registryService ! GetRegistration(agent1)
        val getRegistrationResult = expectMsgClass(classOf[RegistryServiceOperationFailed])
        getRegistrationResult.failure shouldEqual ApiException(ResourceNotFound)
      }
    }

    "servicing a GetRegistration request" should {

      "return the current registration if the agent exists" in withTestData { registryService =>
        registryService ! GetRegistration(agent1)
        val getRegistrationResult = expectMsgClass(classOf[GetRegistrationResult])
        getRegistrationResult.registration shouldEqual registrationHistory5
        getRegistrationResult.metadata shouldEqual metadataHistory5
        getRegistrationResult.lsn shouldEqual 5
      }

      "return ResourceNotFound if the agent doesn't exist" in withTestData { registryService =>
        registryService ! GetRegistration(agent2)
        val getRegistrationResult = expectMsgClass(classOf[RegistryServiceOperationFailed])
        getRegistrationResult.failure shouldEqual ApiException(ResourceNotFound)
      }
    }

    "servicing a GetRegistrationHistory request" should {
      import RegistryManager.{MinGenerationLsn, MaxGenerationLsn}

      "return ResourceNotFound if agent doesn't exist" in withTestData { registryService =>
        registryService ! GetRegistrationHistory(agent2, None, None, 10)
        val getRegistrationHistoryResult = expectMsgClass(classOf[RegistryServiceOperationFailed])
        getRegistrationHistoryResult.failure shouldEqual ApiException(ResourceNotFound)
      }

      "return registration history if the agent exists" in withTestData { registryService =>
        registryService ! GetRegistrationHistory(agent1, Some(MinGenerationLsn), Some(MaxGenerationLsn), 100)
        val getRegistrationHistoryResult = expectMsgClass(classOf[GetRegistrationHistoryResult])
        val page = getRegistrationHistoryResult.page
        page.agents shouldEqual Vector( registrationHistory1, registrationHistory2,
          registrationHistory3, registrationHistory4, registrationHistory5)
        page.last shouldEqual None
        page.exhausted shouldEqual true
      }

      "return the last history as the only element in a page if timeseries parameters are not specified" in withTestData { registryService =>
        registryService ! GetRegistrationHistory(agent1, None, None, 100)
        val getRegistrationHistoryResult = expectMsgClass(classOf[GetRegistrationHistoryResult])
        getRegistrationHistoryResult.page.last shouldEqual None
        getRegistrationHistoryResult.page.exhausted shouldEqual true
        val registration = getRegistrationHistoryResult.page.agents.loneElement
        registration shouldEqual registrationHistory5
      }

      "return a page of history history newer than 'from' when 'from' is specified" in withTestData { registryService =>
        registryService ! GetRegistrationHistory(agent1, Some(GenerationLsn(1,3)), None, 100)
        val getRegistrationHistoryResult = expectMsgClass(classOf[GetRegistrationHistoryResult])
        getRegistrationHistoryResult.page.agents shouldEqual Vector(registrationHistory4, registrationHistory5)
        getRegistrationHistoryResult.page.last shouldEqual None
        getRegistrationHistoryResult.page.exhausted shouldEqual true
      }

      "return a page of history history older than 'to' when 'to' is specified" in withTestData { registryService =>
        registryService ! GetRegistrationHistory(agent1, None, Some(GenerationLsn(1,3)), 100)
        val getRegistrationHistoryResult = expectMsgClass(classOf[GetRegistrationHistoryResult])
        getRegistrationHistoryResult.page.agents shouldEqual Vector(registrationHistory1, registrationHistory2, registrationHistory3)
        getRegistrationHistoryResult.page.last shouldEqual None
        getRegistrationHistoryResult.page.exhausted shouldEqual true
      }

      "return a page of history history between 'from' and 'to' when 'from' and 'to' are specified" in withTestData { registryService =>
        registryService ! GetRegistrationHistory(agent1, Some(GenerationLsn(1,2)), Some(GenerationLsn(1,4)), 100)
        val getRegistrationHistoryResult = expectMsgClass(classOf[GetRegistrationHistoryResult])
        getRegistrationHistoryResult.page.agents shouldEqual Vector(registrationHistory3, registrationHistory4)
        getRegistrationHistoryResult.page.last shouldEqual None
        getRegistrationHistoryResult.page.exhausted shouldEqual true
      }
    }

    "servicing a AddAgentToGroup request" should {

      val metadata = AgentMetadata(agent1, 1, joinedOn = joinedOn, lastUpdate = joinedOn, None)
      val groupName = "foobar"

      "insert agent metadata into a group if the agent has not been added previously" in withRegistryService { registryService =>
        registryService ! AddAgentToGroup(metadata, groupName)
        expectMsgClass(classOf[AddAgentToGroupResult])
        registryService ! DescribeGroup(groupName, 100, None)
        val describeGroupResult = expectMsgClass(classOf[DescribeGroupResult])
        describeGroupResult.page.metadata.loneElement shouldEqual metadata
        describeGroupResult.page.last shouldEqual None
        describeGroupResult.page.exhausted shouldEqual true
      }

      "update agent metadata in a group if the agent has been added previously" in withRegistryService { registryService =>
        registryService ! AddAgentToGroup(metadata, groupName)
        expectMsgClass(classOf[AddAgentToGroupResult])
        registryService ! DescribeGroup(groupName, 100, None)
        val describeGroupResult1 = expectMsgClass(classOf[DescribeGroupResult])
        describeGroupResult1.page.metadata.loneElement shouldEqual metadata
        describeGroupResult1.page.last shouldEqual None
        describeGroupResult1.page.exhausted shouldEqual true

        registryService ! AddAgentToGroup(metadata, groupName)
        expectMsgClass(classOf[AddAgentToGroupResult])
        registryService ! DescribeGroup(groupName, 100, None)
        val describeGroupResult2 = expectMsgClass(classOf[DescribeGroupResult])
        describeGroupResult2.page.metadata.loneElement shouldEqual metadata
        describeGroupResult2.page.last shouldEqual None
        describeGroupResult2.page.exhausted shouldEqual true
      }
    }

    "servicing a RemoveAgentFromGroup request" should {

      val metadata = AgentMetadata(agent1, 1, joinedOn = joinedOn, lastUpdate = joinedOn, None)
      val groupName = "foobar"

      "remove agent metadata from a group if the agent has been added previously" in withRegistryService { registryService =>
        registryService ! AddAgentToGroup(metadata, groupName)
        expectMsgClass(classOf[AddAgentToGroupResult])
        registryService ! DescribeGroup(groupName, 100, None)
        val describeGroupResult1 = expectMsgClass(classOf[DescribeGroupResult])
        describeGroupResult1.page.metadata.loneElement shouldEqual metadata

        registryService ! RemoveAgentFromGroup(metadata.agentId, groupName)
        expectMsgClass(classOf[RemoveAgentFromGroupResult])
        registryService ! DescribeGroup(groupName, 100, None)
        val describeGroupResult2 = expectMsgClass(classOf[DescribeGroupResult])
        describeGroupResult2.page.metadata shouldBe empty
        describeGroupResult2.page.last shouldEqual None
        describeGroupResult2.page.exhausted shouldEqual true
      }

      "do nothing if the agent is not a part of the specified group" in withRegistryService { registryService =>
        registryService ! DescribeGroup("nosuchgroup", 100, None)
        val describeGroupResult = expectMsgClass(classOf[DescribeGroupResult])
        describeGroupResult.page.metadata shouldBe empty
        describeGroupResult.page.last shouldEqual None
        describeGroupResult.page.exhausted shouldEqual true
      }
    }
  }
}
