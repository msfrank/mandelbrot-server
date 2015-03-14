package io.mandelbrot.persistence.cassandra

import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.ActorSystem
import com.datastax.driver.core.Session
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import org.scalatest.ShouldMatchers
import scala.concurrent.duration._
import scala.concurrent.Await

import io.mandelbrot.core.{ResourceNotFound, ApiException, AkkaConfig}
import io.mandelbrot.core.model._
import io.mandelbrot.core.ConfigConversions._
import io.mandelbrot.persistence.cassandra.CassandraPersister.CassandraPersisterSettings

class CommittedIndexDALSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("CommittedIndexDALSpec", AkkaConfig ++ CassandraConfig))

  override def afterAll(): Unit = {
    Cassandra(system).dropKeyspace()
  }

  val settings = CassandraPersisterSettings()

  "A CommittedIndexDAL" should {

    "create the state table during initialization" in {
      val session = Cassandra(system).getSession
      val dal = new CommittedIndexDAL(settings, session, system.dispatcher)
      val keyspaceName = Cassandra(system).keyspaceName
      val keyspaceMeta = session.getCluster.getMetadata.getKeyspace(keyspaceName)
      val table = keyspaceMeta.getTable(dal.tableName)
      table should not be null
      table.getName shouldEqual dal.tableName
    }
  }

  "A CommittedIndexDAL" should {

    var _dal: CommittedIndexDAL = null

    def withSessionAndDAL(testCode: (Session,CommittedIndexDAL) => Any) = {
      val session = Cassandra(system).getSession
      if (_dal == null)
        _dal = new CommittedIndexDAL(settings, session, system.dispatcher)
      Await.result(_dal.flushCommittedIndex(), 5.seconds)
      testCode(session, _dal)
    }

    "initialize committed index" in withSessionAndDAL { (session, dal) =>
      val probeRef = ProbeRef("test:1")
      val timestamp = DateTime.now()
      Await.result(dal.initializeCommittedIndex(probeRef, timestamp), 5.seconds)
      val getCommittedIndexResult = Await.result(dal.getCommittedIndex(probeRef), 5.seconds)
      getCommittedIndexResult.probeRef shouldEqual probeRef
      getCommittedIndexResult.initial shouldEqual timestamp
      getCommittedIndexResult.current shouldEqual timestamp
      getCommittedIndexResult.last shouldEqual None
    }

    "update committed index" in withSessionAndDAL { (session, dal) =>
      val probeRef = ProbeRef("test:2")
      val initial = DateTime.now()
      Await.result(dal.initializeCommittedIndex(probeRef, initial), 5.seconds)
      val timestamp = DateTime.now()
      Await.result(dal.updateCommittedIndex(probeRef, timestamp, initial), 5.seconds)
      val getCommittedIndexResult = Await.result(dal.getCommittedIndex(probeRef), 5.seconds)
      getCommittedIndexResult.probeRef shouldEqual probeRef
      getCommittedIndexResult.initial shouldEqual initial
      getCommittedIndexResult.current shouldEqual timestamp
      getCommittedIndexResult.last shouldEqual Some(initial)
    }

    "get committed index when probe doesn't exist" in withSessionAndDAL { (session, dal) =>
      val probeRef = ProbeRef("test:3")
      val ex = the[ApiException] thrownBy {
        Await.result(dal.getCommittedIndex(probeRef), 5.seconds)
      }
      ex.failure shouldEqual ResourceNotFound
    }

    "delete committed index" in withSessionAndDAL { (session, dal) =>
      val probeRef = ProbeRef("test:5")
      val timestamp = DateTime.now()
      Await.result(dal.initializeCommittedIndex(probeRef, timestamp), 5.seconds)
      val deleteProbeStateResult = Await.result(dal.deleteCommittedIndex(probeRef), 5.seconds)
      val ex = the[ApiException] thrownBy {
        Await.result(dal.getCommittedIndex(probeRef), 5.seconds)
      }
      ex.failure shouldEqual ResourceNotFound
   }
  }
}
