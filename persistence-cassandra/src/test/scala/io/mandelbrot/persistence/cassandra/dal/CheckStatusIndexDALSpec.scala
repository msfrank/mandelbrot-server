package io.mandelbrot.persistence.cassandra.dal

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.datastax.driver.core.Session
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpecLike}
import scala.concurrent.Await
import scala.concurrent.duration._

import io.mandelbrot.core.ConfigConversions._
import io.mandelbrot.core.model._
import io.mandelbrot.core.{AkkaConfig, ApiException, ResourceNotFound}
import io.mandelbrot.persistence.cassandra.{Cassandra, CassandraConfig, CassandraStatePersisterSettings, EpochUtils}

class CheckStatusIndexDALSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("CheckStatusIndexDALSpec", AkkaConfig ++ CassandraConfig))

  override def afterAll(): Unit = {
    Cassandra(system).dropKeyspace()
  }

  val settings = CassandraStatePersisterSettings()

  "A CheckStatusIndexDAL" should {

    "create the check_status_index table during initialization" in {
      val session = Cassandra(system).getSession
      val dal = new CheckStatusIndexDAL(settings, session, system.dispatcher)
      val keyspaceName = Cassandra(system).keyspaceName
      val keyspaceMeta = session.getCluster.getMetadata.getKeyspace(keyspaceName)
      val table = keyspaceMeta.getTable(dal.tableName)
      table should not be null
      table.getName shouldEqual dal.tableName
    }
  }

  "A CheckStatusIndexDAL" should {

    val generation = 1L

    var _dal: CheckStatusIndexDAL = null

    def withSessionAndDAL(testCode: (Session,CheckStatusIndexDAL) => Any) = {
      val session = Cassandra(system).getSession
      if (_dal == null)
        _dal = new CheckStatusIndexDAL(settings, session, system.dispatcher)
      Await.result(_dal.flushIndex(), 5.seconds)
      testCode(session, _dal)
    }

    "put an epoch" in withSessionAndDAL { (session, dal) =>
      val checkRef = CheckRef("test.1:check")
      val timestamp = DateTime.now()
      Await.result(dal.putEpoch(checkRef, generation, timestamp.getMillis), 5.seconds)
    }

    "get the first epoch" in withSessionAndDAL { (session, dal) =>
      val checkRef = CheckRef("test.2:check")
      val epoch1 = DateTime.now()
      Await.result(dal.putEpoch(checkRef, generation, epoch1.getMillis), 5.seconds)
      val epoch2 = epoch1.plusDays(1)
      Await.result(dal.putEpoch(checkRef, generation, epoch2.getMillis), 5.seconds)
      val epoch3 = epoch2.plusDays(1)
      Await.result(dal.putEpoch(checkRef, generation, epoch3.getMillis), 5.seconds)
      val epoch4 = epoch3.plusDays(1)
      Await.result(dal.putEpoch(checkRef, generation, epoch4.getMillis), 5.seconds)
      val epoch5 = epoch4.plusDays(1)
      Await.result(dal.putEpoch(checkRef, generation, epoch5.getMillis), 5.seconds)
      val getFirstEpoch = Await.result(dal.getFirstEpoch(checkRef, generation), 5.seconds)
      getFirstEpoch shouldEqual epoch1.getMillis
    }

    "get the last epoch" in withSessionAndDAL { (session, dal) =>
      val checkRef = CheckRef("test.3:check")
      val epoch1 = DateTime.now()
      Await.result(dal.putEpoch(checkRef, generation, epoch1.getMillis), 5.seconds)
      val epoch2 = epoch1.plusDays(1)
      Await.result(dal.putEpoch(checkRef, generation, epoch2.getMillis), 5.seconds)
      val epoch3 = epoch2.plusDays(1)
      Await.result(dal.putEpoch(checkRef, generation, epoch3.getMillis), 5.seconds)
      val epoch4 = epoch3.plusDays(1)
      Await.result(dal.putEpoch(checkRef, generation, epoch4.getMillis), 5.seconds)
      val epoch5 = epoch4.plusDays(1)
      Await.result(dal.putEpoch(checkRef, generation, epoch5.getMillis), 5.seconds)
      val getLastEpoch = Await.result(dal.getLastEpoch(checkRef, generation), 5.seconds)
      getLastEpoch shouldEqual epoch5.getMillis
    }

    "get list of epochs ascending" in withSessionAndDAL { (session, dal) =>
      val checkRef = CheckRef("test.4:check")
      val epoch1 = DateTime.now()
      Await.result(dal.putEpoch(checkRef, generation, epoch1.getMillis), 5.seconds)
      val epoch2 = epoch1.plusDays(1)
      Await.result(dal.putEpoch(checkRef, generation, epoch2.getMillis), 5.seconds)
      val epoch3 = epoch2.plusDays(1)
      Await.result(dal.putEpoch(checkRef, generation, epoch3.getMillis), 5.seconds)
      val epoch4 = epoch3.plusDays(1)
      Await.result(dal.putEpoch(checkRef, generation, epoch4.getMillis), 5.seconds)
      val epoch5 = epoch4.plusDays(1)
      Await.result(dal.putEpoch(checkRef, generation, epoch5.getMillis), 5.seconds)
      val listEpochsInclusiveAscending = Await.result(dal.listEpochsInclusiveAscending(checkRef,
        generation, EpochUtils.SMALLEST_TIMESTAMP, EpochUtils.LARGEST_TIMESTAMP, limit = 100), 5.seconds)
      listEpochsInclusiveAscending.epochs shouldEqual Vector(
          epoch1.getMillis, epoch2.getMillis, epoch3.getMillis, epoch4.getMillis, epoch5.getMillis
      )
    }

    "get list of epochs descending" in withSessionAndDAL { (session, dal) =>
      val checkRef = CheckRef("test.5:check")
      val epoch1 = DateTime.now()
      Await.result(dal.putEpoch(checkRef, generation, epoch1.getMillis), 5.seconds)
      val epoch2 = epoch1.plusDays(1)
      Await.result(dal.putEpoch(checkRef, generation, epoch2.getMillis), 5.seconds)
      val epoch3 = epoch2.plusDays(1)
      Await.result(dal.putEpoch(checkRef, generation, epoch3.getMillis), 5.seconds)
      val epoch4 = epoch3.plusDays(1)
      Await.result(dal.putEpoch(checkRef, generation, epoch4.getMillis), 5.seconds)
      val epoch5 = epoch4.plusDays(1)
      Await.result(dal.putEpoch(checkRef, generation, epoch5.getMillis), 5.seconds)
      val listEpochsInclusiveDescending = Await.result(dal.listEpochsInclusiveDescending(checkRef,
        generation, EpochUtils.SMALLEST_TIMESTAMP, EpochUtils.LARGEST_TIMESTAMP, limit = 100), 5.seconds)
      listEpochsInclusiveDescending.epochs shouldEqual Vector(
          epoch5.getMillis, epoch4.getMillis, epoch3.getMillis, epoch2.getMillis, epoch1.getMillis
      )
    }

    "delete index for a check" in withSessionAndDAL { (session, dal) =>
      val checkRef = CheckRef("test:5")
      val timestamp = DateTime.now()
      Await.result(dal.putEpoch(checkRef, generation, timestamp.getMillis), 5.seconds)
      val getFirstEpoch = Await.result(dal.getFirstEpoch(checkRef, generation), 5.seconds)
      Await.result(dal.deleteIndex(checkRef, generation), 5.seconds)
      val ex = the[ApiException] thrownBy {
        Await.result(dal.getFirstEpoch(checkRef, generation), 5.seconds)
      }
      ex.failure shouldEqual ResourceNotFound
   }
  }
}
