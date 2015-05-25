package io.mandelbrot.persistence.cassandra.dal

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.datastax.driver.core.Session
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

import io.mandelbrot.core.ConfigConversions._
import io.mandelbrot.core.{AkkaConfig, ApiException, ResourceNotFound}
import io.mandelbrot.persistence.cassandra.{Cassandra, CassandraConfig, CassandraRegistryPersisterSettings}

class AgentTombstoneDALSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("AgentTombstoneDALSpec", AkkaConfig ++ CassandraConfig))

  override def afterAll(): Unit = {
    Cassandra(system).dropKeyspace()
  }

  val settings = CassandraRegistryPersisterSettings()

  "An AgentTombstoneDAL" should {

    "create the entities table during initialization" in {
      val session = Cassandra(system).getSession
      val dal = new AgentTombstoneDAL(settings, session, system.dispatcher)
      val keyspaceName = Cassandra(system).keyspaceName
      val keyspaceMeta = session.getCluster.getMetadata.getKeyspace(keyspaceName)
      val table = keyspaceMeta.getTable(dal.tableName)
      table should not be null
      table.getName shouldEqual dal.tableName
    }
  }

  "An AgentTombstoneDAL" should {

    var _dal: AgentTombstoneDAL = null

    def withSessionAndDAL(testCode: (Session,AgentTombstoneDAL) => Any) = {
      val session = Cassandra(system).getSession
      if (_dal == null)
        _dal = new AgentTombstoneDAL(settings, session, system.dispatcher)
      Await.result(_dal.flushTombstones(), 5.seconds)
      testCode(session, _dal)
    }

    "put a tombstone" in withSessionAndDAL { (session, dal) =>
    }

    "delete a tombstone" in withSessionAndDAL { (session, dal) =>
    }

    "list tombstones older then a specified datetime" in withSessionAndDAL { (session,dal) =>
    }
  }
}
