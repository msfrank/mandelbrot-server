package io.mandelbrot.persistence.cassandra

import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.ActorSystem
import com.datastax.driver.core.Session
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import org.scalatest.ShouldMatchers
import org.joda.time.DateTime
import scala.concurrent.duration._
import scala.concurrent.Await

import io.mandelbrot.core.{ResourceNotFound, ApiException, AkkaConfig}
import io.mandelbrot.core.ConfigConversions._
import io.mandelbrot.core.entity._

class EntitiesDALSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("EntitiesDALSpec", AkkaConfig ++ CassandraConfig))

  override def afterAll(): Unit = {
    Cassandra(system).dropKeyspace()
  }

  val settings = CassandraEntityCoordinatorSettings()

  "A EntitiesDAL" should {

    "create the entities table during initialization" in {
      val session = Cassandra(system).getSession
      val dal = new EntitiesDAL(settings, session, system.dispatcher)
      val keyspaceName = Cassandra(system).keyspaceName
      val keyspaceMeta = session.getCluster.getMetadata.getKeyspace(keyspaceName)
      val table = keyspaceMeta.getTable(dal.tableName)
      table should not be null
      table.getName shouldEqual dal.tableName
    }
  }

  "A EntitiesDAL" should {

    var _dal: EntitiesDAL = null
    def withSessionAndDAL(testCode: (Session,EntitiesDAL) => Any) = {
      import system.dispatcher
      val session = Cassandra(system).getSession
      if (_dal == null)
        _dal = new EntitiesDAL(settings, session, system.dispatcher)
      Await.result(_dal.flushEntities(), 5.seconds)
      testCode(session, _dal)
    }
    "create an entity" in withSessionAndDAL { (session,dal) =>
      val op = CreateEntity(shardId = 0, entityKey = "entity1")
      val timestamp = DateTime.now()
      Await.result(dal.createEntity(op, timestamp), 5.seconds)
      val getEntityResult = Await.result(dal.getEntity(GetEntity(op.shardId, op.entityKey)), 5.seconds)
      getEntityResult.shardId shouldEqual 0
      getEntityResult.entityKey shouldEqual "entity1"
    }

    "delete an entity" in withSessionAndDAL { (session,dal) =>
      val timestamp = DateTime.now()
      Await.result(dal.createEntity(CreateEntity(shardId = 0, entityKey = "entity1"), timestamp), 5.seconds)
      val beforeDelete = Await.result(dal.getEntity(GetEntity(0, "entity1")), 5.seconds)
      beforeDelete.shardId shouldEqual 0
      beforeDelete.entityKey shouldEqual "entity1"
      val op = DeleteEntity(0, "entity1")
      val deleteEntityResult = Await.result(dal.deleteEntity(op), 5.seconds)
      val ex = the[ApiException] thrownBy {
        Await.result(dal.getEntity(GetEntity(0, "entity1")), 5.seconds)
      }
      ex.failure shouldEqual ResourceNotFound
    }

    "list entities for a shard" in withSessionAndDAL { (session,dal) =>
      val timestamp = DateTime.now()
      Await.result(dal.createEntity(
        CreateEntity(shardId = 0, entityKey = "entity1"),
        timestamp), 5.seconds)
      Await.result(dal.createEntity(
        CreateEntity(shardId = 0, entityKey = "entity2"),
        timestamp), 5.seconds)
      Await.result(dal.createEntity(
        CreateEntity(shardId = 0, entityKey = "entity3"),
        timestamp), 5.seconds)
      val op = ListEntities(shardId = 0, limit = 10, None)
      val listEntitiesResult = Await.result(dal.listEntities(op), 5.seconds)
      listEntitiesResult.entities.toSet shouldEqual Set(
        Entity(0, "entity1"),
        Entity(0, "entity2"),
        Entity(0, "entity3")
      )
      listEntitiesResult.token shouldEqual None
    }

    "page through entities for a shard" in withSessionAndDAL { (session,dal) =>
      val timestamp = DateTime.now()
      Await.result(dal.createEntity(
        CreateEntity(shardId = 0, entityKey = "entity1"),
        timestamp), 5.seconds)
      Await.result(dal.createEntity(
        CreateEntity(shardId = 0, entityKey = "entity2"),
        timestamp), 5.seconds)
      Await.result(dal.createEntity(
        CreateEntity(shardId = 0, entityKey = "entity3"),
        timestamp), 5.seconds)
      val listEntitiesResult1 = Await.result(dal.listEntities(ListEntities(shardId = 0, limit = 2, None)), 5.seconds)
      listEntitiesResult1.entities.toSet shouldEqual Set(
        Entity(0, "entity1"),
        Entity(0, "entity2")
      )
      listEntitiesResult1.token shouldEqual Some(Entity(0, "entity2"))
      val listEntitiesResult2 = Await.result(dal.listEntities(ListEntities(shardId = 0, limit = 2, listEntitiesResult1.token)), 5.seconds)
      listEntitiesResult2.entities.toSet shouldEqual Set(Entity(0, "entity3"))
      listEntitiesResult2.token shouldEqual None
    }
  }
}
