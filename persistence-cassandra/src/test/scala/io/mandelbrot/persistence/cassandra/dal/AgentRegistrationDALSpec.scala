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
      Await.result(_dal.flushAgentRegistrations(), 5.seconds)
      testCode(session, _dal)
    }

    "put a registration" in withSessionAndDAL { (session, dal) =>
      val agentId = AgentId("test.foo")
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val agentPolicy = AgentPolicy(5.seconds)

      val registration1 = AgentSpec(agentId, "mandelbrot", agentPolicy, Map.empty, Map.empty, metadata = Map("foo" -> "bar"))
      val metadata1 = AgentMetadata(agentId, generation = 1, timestamp, timestamp, None)
      Await.result(dal.updateAgentRegistration(agentId, generation = 1, lsn = 1, registration1, timestamp, timestamp, None, committed = true), 5.seconds)

      val registration2 = AgentSpec(agentId, "mandelbrot", agentPolicy, Map.empty, Map.empty, metadata = Map("foo" -> "baz"))
      val metadata2 = AgentMetadata(agentId, generation = 1, timestamp, timestamp, None)
      Await.result(dal.updateAgentRegistration(agentId, generation = 1, lsn = 2, registration2, timestamp, timestamp, None, committed = true), 5.seconds)

      val expires = DateTime.now(DateTimeZone.UTC)
      val metadata3 = AgentMetadata(agentId, generation = 1, timestamp, timestamp, Some(expires))
      Await.result(dal.updateAgentRegistration(agentId, generation = 1, lsn = 3, registration2, timestamp, timestamp, Some(expires), committed = true), 5.seconds)

      val getRegistrationResult = Await.result(dal.getLastAgentRegistration(GetRegistration(agentId)), 5.seconds)
      getRegistrationResult.registration shouldEqual registration2
      getRegistrationResult.metadata shouldEqual metadata3
      getRegistrationResult.lsn shouldEqual 3
    }

    "get a registration at a specific point in time" in withSessionAndDAL { (session, dal) =>
      val agentId = AgentId("test.foo")
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val agentPolicy = AgentPolicy(5.seconds)

      val registration1 = AgentSpec(agentId, "mandelbrot", agentPolicy, Map.empty, Map.empty, metadata = Map("foo" -> "bar"))
      val metadata1 = AgentMetadata(agentId, generation = 1, timestamp, timestamp, None)
      Await.result(dal.updateAgentRegistration(agentId, generation = 1, lsn = 1, registration1, timestamp, timestamp, None, committed = true), 5.seconds)

      val registration2 = AgentSpec(agentId, "mandelbrot", agentPolicy, Map.empty, Map.empty, metadata = Map("foo" -> "baz"))
      val metadata2 = AgentMetadata(agentId, generation = 1, timestamp, timestamp, None)
      Await.result(dal.updateAgentRegistration(agentId, generation = 1, lsn = 2, registration2, timestamp, timestamp, None, committed = true), 5.seconds)

      val registration3 = AgentSpec(agentId, "mandelbrot", agentPolicy, Map.empty, Map.empty, metadata = Map("foo" -> "qux"))
      val metadata3 = AgentMetadata(agentId, generation = 1, timestamp, timestamp, None)
      Await.result(dal.updateAgentRegistration(agentId, generation = 1, lsn = 3, registration3, timestamp, timestamp, None, committed = true), 5.seconds)

      val (registrationResult,metadataResult) = Await.result(dal.getAgentRegistration(agentId, generation = 1, lsn = 1), 5.seconds)
      registrationResult shouldEqual registration1
      metadataResult shouldEqual metadata1
    }

    "page through registration history" in withSessionAndDAL { (session,dal) =>
    }
  }
}
