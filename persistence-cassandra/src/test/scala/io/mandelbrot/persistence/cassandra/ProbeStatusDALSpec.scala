package io.mandelbrot.persistence.cassandra

import java.util.UUID

import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.ActorSystem
import com.datastax.driver.core.Session
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import org.scalatest.ShouldMatchers
import org.joda.time.{DateTimeZone, DateTime}
import scala.concurrent.duration._
import scala.concurrent.Await

import io.mandelbrot.core.AkkaConfig
import io.mandelbrot.core.ConfigConversions._
import io.mandelbrot.core.model._

class ProbeStatusDALSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ProbeStatusDALSpec", AkkaConfig ++ CassandraConfig))

  override def afterAll(): Unit = {
    Cassandra(system).dropKeyspace()
  }

  val settings = CassandraStatePersisterSettings()

  "A ProbeStatusDAL" should {

    "create the state table during initialization" in {
      val session = Cassandra(system).getSession
      val dal = new ProbeStatusDAL(settings, session, system.dispatcher)
      val keyspaceName = Cassandra(system).keyspaceName
      val keyspaceMeta = session.getCluster.getMetadata.getKeyspace(keyspaceName)
      val table = keyspaceMeta.getTable(dal.tableName)
      table should not be null
      table.getName shouldEqual dal.tableName
    }
  }

  "A ProbeStatusDAL" should {

    var _dal: ProbeStatusDAL = null

    def withSessionAndDAL(testCode: (Session,ProbeStatusDAL) => Any) = {
      val session = Cassandra(system).getSession
      if (_dal == null)
        _dal = new ProbeStatusDAL(settings, session, system.dispatcher)
      Await.result(_dal.flushProbeStatus(), 5.seconds)
      testCode(session, _dal)
    }

    "update probe status" in withSessionAndDAL { (session, dal) =>
      val probeRef = ProbeRef("test:1")
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val epoch = EpochUtils.timestamp2epoch(timestamp)
      val correlation = UUID.randomUUID()
      val acknowledged = UUID.randomUUID()
      val status = ProbeStatus(timestamp, ProbeKnown, Some("known"), ProbeHealthy, Map("metric"->BigDecimal(0.1)),
        Some(timestamp), Some(timestamp), Some(correlation), Some(acknowledged), squelched = false)
      val notifications = Vector.empty[ProbeNotification]
      Await.result(dal.updateProbeStatus(probeRef, epoch, status, notifications), 5.seconds)
      val getProbeStatusResult = Await.result(dal.getProbeStatus(probeRef, epoch, timestamp), 5.seconds)
      getProbeStatusResult.timestamp shouldEqual timestamp
      getProbeStatusResult.lifecycle shouldEqual ProbeKnown
      getProbeStatusResult.summary shouldEqual Some("known")
      getProbeStatusResult.health shouldEqual ProbeHealthy
      getProbeStatusResult.metrics shouldEqual Map("metric" -> BigDecimal(0.1))
      getProbeStatusResult.lastUpdate shouldEqual Some(timestamp)
      getProbeStatusResult.lastChange shouldEqual Some(timestamp)
      getProbeStatusResult.correlation shouldEqual Some(correlation)
      getProbeStatusResult.acknowledged shouldEqual Some(acknowledged)
      getProbeStatusResult.squelched shouldEqual false
    }

    "get probe condition" in withSessionAndDAL { (session, dal) =>
      val probeRef = ProbeRef("test:2")
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val epoch = EpochUtils.timestamp2epoch(timestamp)
      val correlation = UUID.randomUUID()
      val acknowledged = UUID.randomUUID()
      val status = ProbeStatus(timestamp, ProbeKnown, Some("known"), ProbeHealthy, Map("metric"->BigDecimal(0.1)),
        Some(timestamp), Some(timestamp), Some(correlation), Some(acknowledged), squelched = false)
      val notifications = Vector.empty[ProbeNotification]
      Await.result(dal.updateProbeStatus(probeRef, epoch, status, notifications), 5.seconds)
      val getProbeConditionResult = Await.result(dal.getProbeCondition(probeRef, epoch, timestamp), 5.seconds)
      getProbeConditionResult.timestamp shouldEqual timestamp
      getProbeConditionResult.lifecycle shouldEqual ProbeKnown
      getProbeConditionResult.summary shouldEqual Some("known")
      getProbeConditionResult.health shouldEqual ProbeHealthy
      getProbeConditionResult.correlation shouldEqual Some(correlation)
      getProbeConditionResult.acknowledged shouldEqual Some(acknowledged)
      getProbeConditionResult.squelched shouldEqual false
    }

    "get probe notifications" in withSessionAndDAL { (session, dal) =>
      val probeRef = ProbeRef("test:3")
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val epoch = EpochUtils.timestamp2epoch(timestamp)
      val correlation = UUID.randomUUID()
      val acknowledged = UUID.randomUUID()
      val status = ProbeStatus(timestamp, ProbeKnown, Some("known"), ProbeFailed, Map("metric"->BigDecimal(0.1)),
        Some(timestamp), Some(timestamp), Some(correlation), Some(acknowledged), squelched = false)
      val notifications = Vector(NotifyHealthAlerts(probeRef, timestamp, ProbeFailed, correlation, Some(acknowledged)))
      Await.result(dal.updateProbeStatus(probeRef, epoch, status, notifications), 5.seconds)
      val getProbeNotificationsResult = Await.result(dal.getProbeNotifications(probeRef, epoch, timestamp), 5.seconds)
      getProbeNotificationsResult shouldEqual ProbeNotifications(notifications)
    }

    "get probe metrics" in withSessionAndDAL { (session, dal) =>
      val probeRef = ProbeRef("test:4")
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val epoch = EpochUtils.timestamp2epoch(timestamp)
      val correlation = UUID.randomUUID()
      val acknowledged = UUID.randomUUID()
      val status = ProbeStatus(timestamp, ProbeKnown, Some("known"), ProbeHealthy, Map("metric"->BigDecimal(0.1)),
        Some(timestamp), Some(timestamp), Some(correlation), Some(acknowledged), squelched = false)
      val notifications = Vector.empty[ProbeNotification]
      Await.result(dal.updateProbeStatus(probeRef, epoch, status, notifications), 5.seconds)
      val getProbeMetricsResult = Await.result(dal.getProbeMetrics(probeRef, epoch, timestamp), 5.seconds)
      getProbeMetricsResult shouldEqual ProbeMetrics(status.metrics)
    }
  }
}
