package io.mandelbrot.persistence.cassandra

import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.ActorSystem
import com.datastax.driver.core.Session
import io.mandelbrot.core.notification.ProbeNotification
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

class ConditionsDALSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ConditionsDALSpec", AkkaConfig ++ CassandraConfig))

  override def afterAll(): Unit = {
    Cassandra(system).dropKeyspace()
  }

  val settings = CassandraPersisterSettings()

  "A ConditionsDAL" should {

    "create the state table during initialization" in {
      val session = Cassandra(system).getSession
      val dal = new ConditionsDAL(settings, session)(system.dispatcher)
      val keyspaceName = Cassandra(system).keyspaceName
      val keyspaceMeta = session.getCluster.getMetadata.getKeyspace(keyspaceName)
      val table = keyspaceMeta.getTable(dal.tableName)
      table should not be null
      table.getName shouldEqual dal.tableName
    }
  }

  "A ConditionsDAL" should {

    var _dal: ConditionsDAL = null

    def withSessionAndDAL(testCode: (Session,ConditionsDAL) => Any) = {
      import system.dispatcher
      val session = Cassandra(system).getSession
      if (_dal == null)
        _dal = new ConditionsDAL(settings, session)(system.dispatcher)
      Await.result(_dal.flushConditions(), 5.seconds)
      testCode(session, _dal)
    }

    "insert condition" in withSessionAndDAL { (session, dal) =>
      val probeRef = ProbeRef("test:1")
      val timestamp = DateTime.now()
      val notifications = Vector.empty[ProbeNotification]
      val epoch =
      dal.insertCondition(UpdateProbeStatus(probeRef,
        ProbeStatus(),
        notifications,
        seqNum), epoch)
    }
  }
}
