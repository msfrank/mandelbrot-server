package io.mandelbrot.persistence.cassandra

import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.ActorSystem
import com.datastax.driver.core.Session
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import org.scalatest.matchers.ShouldMatchers
import org.joda.time.DateTime
import scala.concurrent.duration._
import scala.concurrent.Await
import java.net.URI

import io.mandelbrot.core.{ResourceNotFound, ApiException, AkkaConfig}
import io.mandelbrot.core.ConfigConversions._
import io.mandelbrot.core.registry._
import io.mandelbrot.persistence.cassandra.CassandraRegistrar.CassandraRegistrarSettings

class RegistryDALSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("RegistryDALSpec", AkkaConfig ++ CassandraConfig))

  override def afterAll(): Unit = {
    Cassandra(system).dropKeyspace()
  }

  val settings = CassandraRegistrarSettings()

  "A RegistryDAL" should {

    "create the entities table during initialization" in {
      val session = Cassandra(system).getSession
      val dal = new RegistryDAL(settings, session)(system.dispatcher)
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
        _dal = new RegistryDAL(settings, session)(system.dispatcher)
      Await.result(_dal.flushEntities(), 5.seconds)
      testCode(session, _dal)
    }

    "create a probe system" in withSessionAndDAL { (session, dal) =>
      val uri = new URI("test:foo")
      val registration = ProbeRegistration("test", Map.empty, Map.empty, Map.empty)
      val op = CreateProbeSystemEntry(uri, registration)
      val timestamp = DateTime.now()
      Await.result(dal.createProbeSystem(op, timestamp), 5.seconds)
      val getProbeSystemResult = Await.result(dal.getProbeSystem(GetProbeSystemEntry(uri)), 5.seconds)
      getProbeSystemResult.registration shouldEqual registration
      getProbeSystemResult.lsn shouldEqual 1
    }

    "update a probe system" in withSessionAndDAL { (session, dal) =>
      val uri = new URI("test:foo")
      val registration = ProbeRegistration("test", Map.empty, Map.empty, Map.empty)
      val op = UpdateProbeSystemEntry(uri, registration, lsn = 2)
      val timestamp = DateTime.now()
      Await.result(dal.updateProbeSystem(op, timestamp), 5.seconds)
      val getProbeSystemResult = Await.result(dal.getProbeSystem(GetProbeSystemEntry(uri)), 5.seconds)
      getProbeSystemResult.registration shouldEqual registration
      getProbeSystemResult.lsn shouldEqual 3
    }

    "delete a probe system" in withSessionAndDAL { (session, dal) =>
      val uri = new URI("test:foo")
      val registration = ProbeRegistration("test", Map.empty, Map.empty, Map.empty)
      val timestamp = DateTime.now()
      Await.result(dal.createProbeSystem(CreateProbeSystemEntry(uri, registration), timestamp), 5.seconds)
      val getProbeSystemResult = Await.result(dal.getProbeSystem(GetProbeSystemEntry(uri)), 5.seconds)
      val op = DeleteProbeSystemEntry(uri, lsn = 1)
      val deleteEntityResult = Await.result(dal.deleteProbeSystem(op), 5.seconds)
      val ex = evaluating {
        Await.result(dal.getProbeSystem(GetProbeSystemEntry(uri)), 5.seconds)
      } should produce[ApiException]
      ex.failure shouldEqual ResourceNotFound
    }

    "list probe systems" in withSessionAndDAL { (session,dal) =>
      val timestamp = DateTime.now()
      val registration = ProbeRegistration("test", Map.empty, Map.empty, Map.empty)
      val uri1 = new URI("test:1")
      val uri2 = new URI("test:2")
      val uri3 = new URI("test:3")
      Await.result(dal.createProbeSystem(CreateProbeSystemEntry(uri1, registration), timestamp), 5.seconds)
      Await.result(dal.createProbeSystem(CreateProbeSystemEntry(uri2, registration), timestamp), 5.seconds)
      Await.result(dal.createProbeSystem(CreateProbeSystemEntry(uri3, registration), timestamp), 5.seconds)
      val op = ListProbeSystems(10, None)
      val listProbeSystemsResult = Await.result(dal.listProbeSystems(op), 5.seconds)
      listProbeSystemsResult.systems.keySet shouldEqual Set(uri1, uri2, uri3)
      listProbeSystemsResult.token shouldEqual None
    }

    "page through probe systems" in withSessionAndDAL { (session,dal) =>
      val timestamp = DateTime.now()
      val registration = ProbeRegistration("test", Map.empty, Map.empty, Map.empty)
      val uri1 = new URI("test:1")
      val uri2 = new URI("test:2")
      val uri3 = new URI("test:3")
      val uri4 = new URI("test:4")
      val uri5 = new URI("test:5")
      Await.result(dal.createProbeSystem(CreateProbeSystemEntry(uri1, registration), timestamp), 5.seconds)
      Await.result(dal.createProbeSystem(CreateProbeSystemEntry(uri2, registration), timestamp), 5.seconds)
      Await.result(dal.createProbeSystem(CreateProbeSystemEntry(uri3, registration), timestamp), 5.seconds)
      Await.result(dal.createProbeSystem(CreateProbeSystemEntry(uri4, registration), timestamp), 5.seconds)
      Await.result(dal.createProbeSystem(CreateProbeSystemEntry(uri5, registration), timestamp), 5.seconds)
      val listProbeSystemsResult1 = Await.result(dal.listProbeSystems(ListProbeSystems(limit = 2, None)), 5.seconds)
      listProbeSystemsResult1.systems.keySet shouldEqual Set(uri1, uri2)
      listProbeSystemsResult1.token shouldEqual Some(uri2)
      val listProbeSystemsResult2 = Await.result(dal.listProbeSystems(ListProbeSystems(limit = 2, listProbeSystemsResult1.token)), 5.seconds)
      listProbeSystemsResult2.systems.keySet shouldEqual Set(uri3, uri4)
      listProbeSystemsResult2.token shouldEqual Some(uri4)
      val listProbeSystemsResult3 = Await.result(dal.listProbeSystems(ListProbeSystems(limit = 2, listProbeSystemsResult2.token)), 5.seconds)
      listProbeSystemsResult3.systems.keySet shouldEqual Set(uri5)
      listProbeSystemsResult3.token shouldEqual None
    }
  }
}
