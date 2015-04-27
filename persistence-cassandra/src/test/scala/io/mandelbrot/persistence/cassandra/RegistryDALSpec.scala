package io.mandelbrot.persistence.cassandra

import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.ActorSystem
import com.datastax.driver.core.Session
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import org.scalatest.ShouldMatchers
import org.joda.time.DateTime
import scala.concurrent.duration._
import scala.concurrent.Await
import java.net.URI

import io.mandelbrot.core.{ResourceNotFound, ApiException, AkkaConfig}
import io.mandelbrot.core.ConfigConversions._
import io.mandelbrot.core.model._
import io.mandelbrot.core.registry._

class RegistryDALSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("RegistryDALSpec", AkkaConfig ++ CassandraConfig))

  override def afterAll(): Unit = {
    Cassandra(system).dropKeyspace()
  }

  val settings = CassandraRegistryPersisterSettings()

  "A RegistryDAL" should {

    "create the entities table during initialization" in {
      val session = Cassandra(system).getSession
      val dal = new RegistryDAL(settings, session, system.dispatcher)
      val keyspaceName = Cassandra(system).keyspaceName
      val keyspaceMeta = session.getCluster.getMetadata.getKeyspace(keyspaceName)
      val table = keyspaceMeta.getTable(dal.tableName)
      table should not be null
      table.getName shouldEqual dal.tableName
    }
  }

  "A RegistryDAL" should {

    var _dal: RegistryDAL = null

    def withSessionAndDAL(testCode: (Session,RegistryDAL) => Any) = {
      import system.dispatcher
      val session = Cassandra(system).getSession
      if (_dal == null)
        _dal = new RegistryDAL(settings, session, system.dispatcher)
      Await.result(_dal.flushEntities(), 5.seconds)
      testCode(session, _dal)
    }

    "create a probe system" in withSessionAndDAL { (session, dal) =>
      val agentId = AgentId("test.foo")
      val registration = AgentRegistration(agentId, "mandelbrot", Map.empty, Map.empty, Map.empty)
      val op = CreateRegistration(agentId, registration)
      val timestamp = DateTime.now()
      Await.result(dal.createProbeSystem(op, timestamp), 5.seconds)
      val getProbeSystemResult = Await.result(dal.getProbeSystem(GetRegistration(agentId)), 5.seconds)
      getProbeSystemResult.registration shouldEqual registration
      getProbeSystemResult.lsn shouldEqual 1
    }

    "update a probe system" in withSessionAndDAL { (session, dal) =>
      val agentId = AgentId("test.foo")
      val registration = AgentRegistration(agentId, "mandelbrot", Map.empty, Map.empty, Map.empty)
      val op = UpdateRegistration(agentId, registration, lsn = 2)
      val timestamp = DateTime.now()
      Await.result(dal.updateProbeSystem(op, timestamp), 5.seconds)
      val getProbeSystemResult = Await.result(dal.getProbeSystem(GetRegistration(agentId)), 5.seconds)
      getProbeSystemResult.registration shouldEqual registration
      getProbeSystemResult.lsn shouldEqual 3
    }

    "delete a probe system" in withSessionAndDAL { (session, dal) =>
      val agentId = AgentId("test.foo")
      val registration = AgentRegistration(agentId, "mandelbrot", Map.empty, Map.empty, Map.empty)
      val timestamp = DateTime.now()
      Await.result(dal.createProbeSystem(CreateRegistration(agentId, registration), timestamp), 5.seconds)
      val getProbeSystemResult = Await.result(dal.getProbeSystem(GetRegistration(agentId)), 5.seconds)
      val op = DeleteRegistration(agentId, lsn = 1)
      val deleteEntityResult = Await.result(dal.deleteProbeSystem(op), 5.seconds)
      val ex = the[ApiException] thrownBy {
        Await.result(dal.getProbeSystem(GetRegistration(agentId)), 5.seconds)
      }
      ex.failure shouldEqual ResourceNotFound
    }

    "list probe systems" in withSessionAndDAL { (session,dal) =>
      val timestamp = DateTime.now()
      val agent1 = AgentId("test.1")
      val registration1 = AgentRegistration(agent1, "mandelbrot", Map.empty, Map.empty, Map.empty)
      val agent2 = AgentId("test.2")
      val registration2 = AgentRegistration(agent2, "mandelbrot", Map.empty, Map.empty, Map.empty)
      val agent3 = AgentId("test.3")
      val registration3 = AgentRegistration(agent3, "mandelbrot", Map.empty, Map.empty, Map.empty)
      Await.result(dal.createProbeSystem(CreateRegistration(agent1, registration1), timestamp), 5.seconds)
      Await.result(dal.createProbeSystem(CreateRegistration(agent2, registration2), timestamp), 5.seconds)
      Await.result(dal.createProbeSystem(CreateRegistration(agent3, registration3), timestamp), 5.seconds)
      val op = ListRegistrations(10, None)
      val listProbeSystemsResult = Await.result(dal.listProbeSystems(op), 5.seconds)
      listProbeSystemsResult.page.agents.map(_.agentId).toSet shouldEqual Set(agent1, agent2, agent3)
      listProbeSystemsResult.page.last shouldEqual None
    }

    "page through probe systems" in withSessionAndDAL { (session,dal) =>
      val timestamp = DateTime.now()
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
      Await.result(dal.createProbeSystem(CreateRegistration(agent1, registration1), timestamp), 5.seconds)
      Await.result(dal.createProbeSystem(CreateRegistration(agent2, registration2), timestamp), 5.seconds)
      Await.result(dal.createProbeSystem(CreateRegistration(agent3, registration3), timestamp), 5.seconds)
      Await.result(dal.createProbeSystem(CreateRegistration(agent4, registration4), timestamp), 5.seconds)
      Await.result(dal.createProbeSystem(CreateRegistration(agent5, registration5), timestamp), 5.seconds)
      val listProbeSystemsResult1 = Await.result(dal.listProbeSystems(ListRegistrations(limit = 2, None)), 5.seconds)
      listProbeSystemsResult1.page.agents.map(_.agentId).toSet shouldEqual Set(agent1, agent2)
      listProbeSystemsResult1.page.last shouldEqual Some(agent2.toString)
      val listProbeSystemsResult2 = Await.result(dal.listProbeSystems(ListRegistrations(limit = 2, listProbeSystemsResult1.page.last)), 5.seconds)
      listProbeSystemsResult2.page.agents.map(_.agentId).toSet shouldEqual Set(agent3, agent4)
      listProbeSystemsResult2.page.last shouldEqual Some(agent4.toString)
      val listProbeSystemsResult3 = Await.result(dal.listProbeSystems(ListRegistrations(limit = 2, listProbeSystemsResult2.page.last)), 5.seconds)
      listProbeSystemsResult3.page.agents.map(_.agentId).toSet shouldEqual Set(agent5)
      listProbeSystemsResult3.page.last shouldEqual None
    }
  }
}
