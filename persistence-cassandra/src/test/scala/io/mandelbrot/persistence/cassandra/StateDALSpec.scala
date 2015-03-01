package io.mandelbrot.persistence.cassandra

import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.ActorSystem
import com.datastax.driver.core.Session
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import org.scalatest.matchers.ShouldMatchers
import org.joda.time.DateTime
import scala.concurrent.duration._
import scala.concurrent.Await

import io.mandelbrot.core.{ResourceNotFound, ApiException, AkkaConfig}
import io.mandelbrot.core.ConfigConversions._
import io.mandelbrot.core.state.{DeleteProbeStatus, UpdateProbeStatus, InitializeProbeStatus}
import io.mandelbrot.core.system._
import io.mandelbrot.persistence.cassandra.CassandraPersister.CassandraPersisterSettings

class StateDALSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("StateDALSpec", AkkaConfig ++ CassandraConfig))

  override def afterAll(): Unit = {
    Cassandra(system).dropKeyspace()
  }

  val settings = CassandraPersisterSettings()

  "A StateDAL" should {

    "create the state table during initialization" in {
      val session = Cassandra(system).getSession
      val dal = new StateDAL(settings, session)(system.dispatcher)
      val keyspaceName = Cassandra(system).keyspaceName
      val keyspaceMeta = session.getCluster.getMetadata.getKeyspace(keyspaceName)
      val table = keyspaceMeta.getTable(dal.tableName)
      table should not be null
      table.getName shouldEqual dal.tableName
    }
  }

  "A StateDAL" should {

    var _dal: StateDAL = null

    def withSessionAndDAL(testCode: (Session,StateDAL) => Any) = {
      import system.dispatcher
      val session = Cassandra(system).getSession
      if (_dal == null)
        _dal = new StateDAL(settings, session)(system.dispatcher)
      Await.result(_dal.flushProbeState(), 5.seconds)
      testCode(session, _dal)
    }

    "initialize probe state when state doesn't exist" in withSessionAndDAL { (session, dal) =>
      val probeRef = ProbeRef("test:1")
      val timestamp = DateTime.now()
      val op = InitializeProbeStatus(probeRef, timestamp)
      val initializeProbeStateResult = Await.result(dal.initializeProbeState(op.probeRef), 5.seconds)
      initializeProbeStateResult.probeRef shouldEqual probeRef
      initializeProbeStateResult.seqNum shouldEqual 0
      initializeProbeStateResult.timestamp shouldEqual timestamp
      initializeProbeStateResult.lastUpdate shouldEqual None
      initializeProbeStateResult.lastChange shouldEqual None
      val ex = the[ApiException] thrownBy {
        Await.result(dal.getProbeState(probeRef), 5.seconds)
      }
      ex.failure shouldEqual ResourceNotFound
    }

    "initialize probe state when state exists" in withSessionAndDAL { (session, dal) =>
      val probeRef = ProbeRef("test:2")
      val timestamp = DateTime.now()
      val state = ProbeState(probeRef, 3, timestamp, Some(timestamp), Some(timestamp))
      Await.result(dal.updateProbeState(state), 5.seconds)
      val initializeProbeStateResult = Await.result(dal.initializeProbeState(probeRef), 5.seconds)
      initializeProbeStateResult.probeRef shouldEqual probeRef
      initializeProbeStateResult.seqNum shouldEqual 3
      initializeProbeStateResult.timestamp shouldEqual timestamp
      initializeProbeStateResult.lastUpdate shouldEqual Some(timestamp)
      initializeProbeStateResult.lastChange shouldEqual Some(timestamp)
    }

    "update probe state" in withSessionAndDAL { (session, dal) =>
      val probeRef = ProbeRef("test:3")
      val timestamp = DateTime.now()
      val state = ProbeState(probeRef, 3, timestamp, Some(timestamp), Some(timestamp))
      Await.result(dal.updateProbeState(state), 5.seconds)
      val initializeProbeStateResult = Await.result(dal.getProbeState(probeRef), 5.seconds)
      initializeProbeStateResult.probeRef shouldEqual probeRef
      initializeProbeStateResult.seqNum shouldEqual 3
      initializeProbeStateResult.timestamp shouldEqual timestamp
      initializeProbeStateResult.lastUpdate shouldEqual Some(timestamp)
      initializeProbeStateResult.lastChange shouldEqual Some(timestamp)
    }

    "delete probe state" in withSessionAndDAL { (session, dal) =>
      val probeRef = ProbeRef("test:4")
      val timestamp = DateTime.now()
      val state = ProbeState(probeRef, 3, timestamp, Some(timestamp), Some(timestamp))
      Await.result(dal.updateProbeState(state), 5.seconds)
      val deleteProbeStateResult = Await.result(dal.deleteProbeState(probeRef), 5.seconds)
      val ex = the[ApiException] thrownBy {
        Await.result(dal.getProbeState(probeRef), 5.seconds)
      }
      ex.failure shouldEqual ResourceNotFound
   }
  }
}
