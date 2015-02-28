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
import io.mandelbrot.core.state.{DeleteProbeStatus, UpdateProbeStatus, GetProbeState, InitializeProbeStatus}
import io.mandelbrot.core.system._
import io.mandelbrot.persistence.cassandra.CassandraPersister.CassandraPersisterSettings

class StateDALSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("StateDALSpec", AkkaConfig ++ CassandraConfig))

  override def afterAll(): Unit = {
    Cassandra(system).dropKeyspace()
  }

  val settings = CassandraPersisterSettings()

  "A StateDAL" should {

    "create the entities table during initialization" in {
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
      val op = InitializeProbeStatus(probeRef, timestamp, lsn = 1)
      val initializeProbeStateResult = Await.result(dal.initializeProbeState(op), 5.seconds)
      initializeProbeStateResult.status.lifecycle shouldEqual ProbeInitializing
      initializeProbeStateResult.status.health shouldEqual ProbeUnknown
      initializeProbeStateResult.lsn shouldEqual 0
      val ex = evaluating {
        Await.result(dal.getProbeState(GetProbeState(probeRef)), 5.seconds)
      } should produce[ApiException]
      ex.failure shouldEqual ResourceNotFound
    }

    "initialize probe state when state exists" in withSessionAndDAL { (session, dal) =>
      val probeRef = ProbeRef("test:2")
      val timestamp = DateTime.now()
      val status = ProbeStatus(probeRef, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      val op = UpdateProbeStatus(probeRef, status, lsn = 3)
      Await.result(dal.updateProbeState(op), 5.seconds)
      val initializeProbeStateResult = Await.result(dal.initializeProbeState(InitializeProbeStatus(probeRef, timestamp, lsn = 0)), 5.seconds)
      initializeProbeStateResult.status shouldEqual status
      initializeProbeStateResult.lsn shouldEqual 3
    }

    "update probe state" in withSessionAndDAL { (session, dal) =>
      val probeRef = ProbeRef("test:3")
      val timestamp = DateTime.now()
      val status = ProbeStatus(probeRef, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      val op = UpdateProbeStatus(probeRef, status, lsn = 3)
      Await.result(dal.updateProbeState(op), 5.seconds)
      val getProbeStateResult = Await.result(dal.getProbeState(GetProbeState(probeRef)), 5.seconds)
      getProbeStateResult.status shouldEqual status
      getProbeStateResult.lsn shouldEqual 3
    }

    "delete probe state" in withSessionAndDAL { (session, dal) =>
      val probeRef = ProbeRef("test:4")
      val timestamp = DateTime.now()
      val status = ProbeStatus(probeRef, timestamp, ProbeKnown, ProbeHealthy, None, None, None, None, None, false)
      Await.result(dal.updateProbeState(UpdateProbeStatus(probeRef, status, lsn = 3)), 5.seconds)
      val op = DeleteProbeStatus(probeRef, None, lsn = 3)
      val deleteProbeStateResult = Await.result(dal.deleteProbeState(op), 5.seconds)
      val ex = evaluating {
        Await.result(dal.getProbeState(GetProbeState(probeRef)), 5.seconds)
      } should produce[ApiException]
      ex.failure shouldEqual ResourceNotFound
   }
  }
}
