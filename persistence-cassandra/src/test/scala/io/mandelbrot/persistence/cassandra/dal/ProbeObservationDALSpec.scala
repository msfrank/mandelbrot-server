package io.mandelbrot.persistence.cassandra.dal

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.datastax.driver.core.Session
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpecLike}
import org.scalatest.Inside._
import scala.concurrent.Await
import scala.concurrent.duration._

import io.mandelbrot.core.AkkaConfig
import io.mandelbrot.core.ConfigConversions._
import io.mandelbrot.core.model._
import io.mandelbrot.persistence.cassandra.{Cassandra, CassandraConfig, CassandraStatePersisterSettings, EpochUtils}

class ProbeObservationDALSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ProbeObservationDALSpec", AkkaConfig ++ CassandraConfig))

  override def afterAll(): Unit = {
    Cassandra(system).dropKeyspace()
  }

  val settings = CassandraStatePersisterSettings()

  "A ProbeObservationDAL" should {

    "create the state table during initialization" in {
      val session = Cassandra(system).getSession
      val dal = new ProbeObservationDAL(settings, session, system.dispatcher)
      val keyspaceName = Cassandra(system).keyspaceName
      val keyspaceMeta = session.getCluster.getMetadata.getKeyspace(keyspaceName)
      val table = keyspaceMeta.getTable(dal.tableName)
      table should not be null
      table.getName shouldEqual dal.tableName
    }
  }

  "A ProbeObservationDAL" should {

    val generation = 1L

    var _dal: ProbeObservationDAL = null

    def withSessionAndDAL(testCode: (Session,ProbeObservationDAL) => Any) = {
      val session = Cassandra(system).getSession
      if (_dal == null)
        _dal = new ProbeObservationDAL(settings, session, system.dispatcher)
      Await.result(_dal.flushProbeObservation(), 5.seconds)
      testCode(session, _dal)
    }

    "update probe observation" in withSessionAndDAL { (session, dal) =>
      val probeRef = ProbeRef("test.local.1:check")
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val epoch = EpochUtils.timestamp2epoch(timestamp)
      val observation = ScalarMapObservation(timestamp, Map("metric"->BigDecimal(0.1)))
      Await.result(dal.updateProbeObservation(probeRef, generation, epoch, observation), 5.seconds)
      val getProbeObservationResult = Await.result(dal.getProbeObservation(probeRef, generation, epoch, timestamp), 5.seconds)
      getProbeObservationResult.generation shouldEqual generation
      getProbeObservationResult.observation shouldEqual observation
    }

    "get probe observation" in withSessionAndDAL { (session, dal) =>
      val probeRef = ProbeRef("test.local.2:check")
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val epoch = EpochUtils.timestamp2epoch(timestamp)
      val observation = ScalarMapObservation(timestamp, Map("metric"->BigDecimal(0.1)))
      Await.result(dal.updateProbeObservation(probeRef, generation, epoch, observation), 5.seconds)
      val getProbeObservationResult = Await.result(dal.getProbeObservation(probeRef, generation, epoch, timestamp), 5.seconds)
      getProbeObservationResult.generation shouldEqual generation
      getProbeObservationResult.observation shouldEqual observation
    }
  }
}
