package io.mandelbrot.persistence.cassandra.dal

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.datastax.driver.core.Session
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

import io.mandelbrot.core.ConfigConversions._
import io.mandelbrot.core.model._
import io.mandelbrot.core.registry._
import io.mandelbrot.core.{AkkaConfig, ApiException, ResourceNotFound}
import io.mandelbrot.persistence.cassandra.{Cassandra, CassandraConfig, CassandraRegistryPersisterSettings}

class AgentRegistrationDALSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("AgentRegistrationDALSpec", AkkaConfig ++ CassandraConfig))

  override def afterAll(): Unit = {
    Cassandra(system).dropKeyspace()
  }

  val settings = CassandraRegistryPersisterSettings()

  "An AgentRegistrationDAL" should {

    "create the entities table during initialization" in {
      val session = Cassandra(system).getSession
      val dal = new AgentRegistrationDAL(settings, session, system.dispatcher)
      val keyspaceName = Cassandra(system).keyspaceName
      val keyspaceMeta = session.getCluster.getMetadata.getKeyspace(keyspaceName)
      val table = keyspaceMeta.getTable(dal.tableName)
      table should not be null
      table.getName shouldEqual dal.tableName
    }
  }

  "An AgentRegistrationDAL" should {

    var _dal: AgentRegistrationDAL = null

    def withSessionAndDAL(testCode: (Session,AgentRegistrationDAL) => Any) = {
      val session = Cassandra(system).getSession
      if (_dal == null)
        _dal = new AgentRegistrationDAL(settings, session, system.dispatcher)
      Await.result(_dal.flushEntities(), 5.seconds)
      testCode(session, _dal)
    }

    "create a check system" in withSessionAndDAL { (session, dal) =>
      val agentId = AgentId("test.foo")
      val registration = AgentRegistration(agentId, "mandelbrot", Map.empty, Map.empty, Map.empty)
      val op = CreateRegistration(agentId, registration)
      val timestamp = DateTime.now(DateTimeZone.UTC)
      Await.result(dal.createAgent(op, timestamp), 5.seconds)
      val getAgentResult = Await.result(dal.getAgent(GetRegistration(agentId)), 5.seconds)
      getAgentResult.registration shouldEqual registration
      getAgentResult.metadata.agentId shouldEqual agentId
      getAgentResult.metadata.joinedOn shouldEqual timestamp
      getAgentResult.metadata.lastUpdate shouldEqual timestamp
      getAgentResult.metadata.lsn shouldEqual 1
    }

    "update a check system" in withSessionAndDAL { (session, dal) =>
      val agentId = AgentId("test.foo")
      val registration = AgentRegistration(agentId, "mandelbrot", Map.empty, Map.empty, Map.empty)
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val metadata = AgentMetadata(agentId, timestamp, timestamp, 2)
      val op = UpdateRegistration(agentId, registration, metadata)
      Await.result(dal.updateAgent(op), 5.seconds)
      val getAgentResult = Await.result(dal.getAgent(GetRegistration(agentId)), 5.seconds)
      getAgentResult.registration shouldEqual registration
      getAgentResult.metadata shouldEqual metadata
    }

    "delete a check system" in withSessionAndDAL { (session, dal) =>
      val agentId = AgentId("test.foo")
      val registration = AgentRegistration(agentId, "mandelbrot", Map.empty, Map.empty, Map.empty)
      val timestamp = DateTime.now(DateTimeZone.UTC)
      Await.result(dal.createAgent(CreateRegistration(agentId, registration), timestamp), 5.seconds)
      val getAgentResult = Await.result(dal.getAgent(GetRegistration(agentId)), 5.seconds)
      val op = DeleteRegistration(agentId)
      val deleteEntityResult = Await.result(dal.deleteAgent(op), 5.seconds)
      val ex = the[ApiException] thrownBy {
        Await.result(dal.getAgent(GetRegistration(agentId)), 5.seconds)
      }
      ex.failure shouldEqual ResourceNotFound
    }

    "list check systems" in withSessionAndDAL { (session,dal) =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val agent1 = AgentId("test.1")
      val registration1 = AgentRegistration(agent1, "mandelbrot", Map.empty, Map.empty, Map.empty)
      val agent2 = AgentId("test.2")
      val registration2 = AgentRegistration(agent2, "mandelbrot", Map.empty, Map.empty, Map.empty)
      val agent3 = AgentId("test.3")
      val registration3 = AgentRegistration(agent3, "mandelbrot", Map.empty, Map.empty, Map.empty)
      Await.result(dal.createAgent(CreateRegistration(agent1, registration1), timestamp), 5.seconds)
      Await.result(dal.createAgent(CreateRegistration(agent2, registration2), timestamp), 5.seconds)
      Await.result(dal.createAgent(CreateRegistration(agent3, registration3), timestamp), 5.seconds)
      val op = ListRegistrations(10, None)
      val listAgentsResult = Await.result(dal.listAgents(op), 5.seconds)
      listAgentsResult.page.agents.map(_.agentId).toSet shouldEqual Set(agent1, agent2, agent3)
      listAgentsResult.page.last shouldEqual None
    }

    "page through check systems" in withSessionAndDAL { (session,dal) =>
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val agent1 = AgentId("test.1")
      val registration1 = AgentRegistration(agent1, "mandelbrot", Map.empty, Map.empty, Map.empty)
      val agent2 = AgentId("test.2")
      val registration2 = AgentRegistration(agent2, "mandelbrot", Map.empty, Map.empty, Map.empty)
      val agent3 = AgentId("test.3")
      val registration3 = AgentRegistration(agent3, "mandelbrot", Map.empty, Map.empty, Map.empty)
      val agent4 = AgentId("test.4")
      val registration4 = AgentRegistration(agent4, "mandelbrot", Map.empty, Map.empty, Map.empty)
      val agent5 = AgentId("test.5")
      val registration5 = AgentRegistration(agent5, "mandelbrot", Map.empty, Map.empty, Map.empty)
      Await.result(dal.createAgent(CreateRegistration(agent1, registration1), timestamp), 5.seconds)
      Await.result(dal.createAgent(CreateRegistration(agent2, registration2), timestamp), 5.seconds)
      Await.result(dal.createAgent(CreateRegistration(agent3, registration3), timestamp), 5.seconds)
      Await.result(dal.createAgent(CreateRegistration(agent4, registration4), timestamp), 5.seconds)
      Await.result(dal.createAgent(CreateRegistration(agent5, registration5), timestamp), 5.seconds)
      val listAgentsResult1 = Await.result(dal.listAgents(ListRegistrations(limit = 2, None)), 5.seconds)
      listAgentsResult1.page.agents.map(_.agentId).toSet shouldEqual Set(agent1, agent2)
      listAgentsResult1.page.last shouldEqual Some(agent2.toString)
      val listAgentsResult2 = Await.result(dal.listAgents(ListRegistrations(limit = 2, listAgentsResult1.page.last)), 5.seconds)
      listAgentsResult2.page.agents.map(_.agentId).toSet shouldEqual Set(agent3, agent4)
      listAgentsResult2.page.last shouldEqual Some(agent4.toString)
      val listAgentsResult3 = Await.result(dal.listAgents(ListRegistrations(limit = 2, listAgentsResult2.page.last)), 5.seconds)
      listAgentsResult3.page.agents.map(_.agentId).toSet shouldEqual Set(agent5)
      listAgentsResult3.page.last shouldEqual None
    }
  }
}
