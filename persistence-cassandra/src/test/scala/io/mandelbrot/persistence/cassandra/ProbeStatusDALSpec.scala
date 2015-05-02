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

class CheckStatusDALSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("CheckStatusDALSpec", AkkaConfig ++ CassandraConfig))

  override def afterAll(): Unit = {
    Cassandra(system).dropKeyspace()
  }

  val settings = CassandraStatePersisterSettings()

  "A CheckStatusDAL" should {

    "create the state table during initialization" in {
      val session = Cassandra(system).getSession
      val dal = new CheckStatusDAL(settings, session, system.dispatcher)
      val keyspaceName = Cassandra(system).keyspaceName
      val keyspaceMeta = session.getCluster.getMetadata.getKeyspace(keyspaceName)
      val table = keyspaceMeta.getTable(dal.tableName)
      table should not be null
      table.getName shouldEqual dal.tableName
    }
  }

  "A CheckStatusDAL" should {

    var _dal: CheckStatusDAL = null

    def withSessionAndDAL(testCode: (Session,CheckStatusDAL) => Any) = {
      val session = Cassandra(system).getSession
      if (_dal == null)
        _dal = new CheckStatusDAL(settings, session, system.dispatcher)
      Await.result(_dal.flushCheckStatus(), 5.seconds)
      testCode(session, _dal)
    }

    "update check status" in withSessionAndDAL { (session, dal) =>
      val checkRef = CheckRef("test:1")
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val epoch = EpochUtils.timestamp2epoch(timestamp)
      val correlation = UUID.randomUUID()
      val acknowledged = UUID.randomUUID()
      val status = CheckStatus(timestamp, CheckKnown, Some("known"), CheckHealthy, Map("metric"->BigDecimal(0.1)),
        Some(timestamp), Some(timestamp), Some(correlation), Some(acknowledged), squelched = false)
      val notifications = Vector.empty[CheckNotification]
      Await.result(dal.updateCheckStatus(checkRef, epoch, status, notifications), 5.seconds)
      val getCheckStatusResult = Await.result(dal.getCheckStatus(checkRef, epoch, timestamp), 5.seconds)
      getCheckStatusResult.timestamp shouldEqual timestamp
      getCheckStatusResult.lifecycle shouldEqual CheckKnown
      getCheckStatusResult.summary shouldEqual Some("known")
      getCheckStatusResult.health shouldEqual CheckHealthy
      getCheckStatusResult.metrics shouldEqual Map("metric" -> BigDecimal(0.1))
      getCheckStatusResult.lastUpdate shouldEqual Some(timestamp)
      getCheckStatusResult.lastChange shouldEqual Some(timestamp)
      getCheckStatusResult.correlation shouldEqual Some(correlation)
      getCheckStatusResult.acknowledged shouldEqual Some(acknowledged)
      getCheckStatusResult.squelched shouldEqual false
    }

    "get check condition" in withSessionAndDAL { (session, dal) =>
      val checkRef = CheckRef("test:2")
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val epoch = EpochUtils.timestamp2epoch(timestamp)
      val correlation = UUID.randomUUID()
      val acknowledged = UUID.randomUUID()
      val status = CheckStatus(timestamp, CheckKnown, Some("known"), CheckHealthy, Map("metric"->BigDecimal(0.1)),
        Some(timestamp), Some(timestamp), Some(correlation), Some(acknowledged), squelched = false)
      val notifications = Vector.empty[CheckNotification]
      Await.result(dal.updateCheckStatus(checkRef, epoch, status, notifications), 5.seconds)
      val getCheckConditionResult = Await.result(dal.getCheckCondition(checkRef, epoch, timestamp), 5.seconds)
      getCheckConditionResult.timestamp shouldEqual timestamp
      getCheckConditionResult.lifecycle shouldEqual CheckKnown
      getCheckConditionResult.summary shouldEqual Some("known")
      getCheckConditionResult.health shouldEqual CheckHealthy
      getCheckConditionResult.correlation shouldEqual Some(correlation)
      getCheckConditionResult.acknowledged shouldEqual Some(acknowledged)
      getCheckConditionResult.squelched shouldEqual false
    }

    "get check notifications" in withSessionAndDAL { (session, dal) =>
      val checkRef = CheckRef("test:3")
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val epoch = EpochUtils.timestamp2epoch(timestamp)
      val correlation = UUID.randomUUID()
      val acknowledged = UUID.randomUUID()
      val status = CheckStatus(timestamp, CheckKnown, Some("known"), CheckFailed, Map("metric"->BigDecimal(0.1)),
        Some(timestamp), Some(timestamp), Some(correlation), Some(acknowledged), squelched = false)
      val notifications = Vector(NotifyHealthAlerts(checkRef, timestamp, CheckFailed, correlation, Some(acknowledged)))
      Await.result(dal.updateCheckStatus(checkRef, epoch, status, notifications), 5.seconds)
      val getCheckNotificationsResult = Await.result(dal.getCheckNotifications(checkRef, epoch, timestamp), 5.seconds)
      getCheckNotificationsResult shouldEqual CheckNotifications(timestamp, notifications)
    }

    "get check metrics" in withSessionAndDAL { (session, dal) =>
      val checkRef = CheckRef("test:4")
      val timestamp = DateTime.now(DateTimeZone.UTC)
      val epoch = EpochUtils.timestamp2epoch(timestamp)
      val correlation = UUID.randomUUID()
      val acknowledged = UUID.randomUUID()
      val status = CheckStatus(timestamp, CheckKnown, Some("known"), CheckHealthy, Map("metric"->BigDecimal(0.1)),
        Some(timestamp), Some(timestamp), Some(correlation), Some(acknowledged), squelched = false)
      val notifications = Vector.empty[CheckNotification]
      Await.result(dal.updateCheckStatus(checkRef, epoch, status, notifications), 5.seconds)
      val getCheckMetricsResult = Await.result(dal.getCheckMetrics(checkRef, epoch, timestamp), 5.seconds)
      getCheckMetricsResult shouldEqual CheckMetrics(timestamp, status.metrics)
    }
  }
}
