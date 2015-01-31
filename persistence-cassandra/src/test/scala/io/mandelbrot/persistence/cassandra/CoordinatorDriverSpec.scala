package io.mandelbrot.persistence.cassandra

import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.{Address, ActorSystem}
import com.datastax.driver.core.Session
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import org.scalatest.matchers.ShouldMatchers
import org.joda.time.DateTime
import scala.concurrent.duration._

import io.mandelbrot.core.{ResourceNotFound, Conflict, ApiException, AkkaConfig}
import io.mandelbrot.core.ConfigConversions._
import io.mandelbrot.core.cluster._
import io.mandelbrot.persistence.cassandra.CassandraCoordinator.CassandraCoordinatorSettings

import scala.concurrent.{Future, Await}

class CoordinatorDriverSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("CoordinatorDriverSpec", AkkaConfig ++ CassandraConfig))

  override def afterAll(): Unit = {
    Cassandra(system).dropKeyspace()
  }

  val settings = CassandraCoordinatorSettings()

  "A CoordinatorDriver" should {

    "create the shards and entities tables during initialization" in {
      val session = Cassandra(system).getSession
      val driver = new CoordinatorDriver(settings, session)(system.dispatcher)
      val keyspaceName = Cassandra(system).keyspaceName
      val keyspaceMeta = session.getCluster.getMetadata.getKeyspace(keyspaceName)
      keyspaceMeta.getTable("shards").getName shouldEqual "shards"
      keyspaceMeta.getTable("entities").getName shouldEqual "entities"
    }
  }

  "A CoordinatorDriver" should {

    var _driver: CoordinatorDriver = null
    def withSessionAndDriver(testCode: (Session,CoordinatorDriver) => Any) = {
      import system.dispatcher
      val session = Cassandra(system).getSession
      if (_driver == null)
        _driver = new CoordinatorDriver(settings, session)(system.dispatcher)
      Await.result(Future.sequence(Seq(_driver.flushShards(), _driver.flushEntities())), 5.seconds)
      testCode(session, _driver)
    }

    "create a shard" in withSessionAndDriver { (session,driver) =>
      val op = CreateShard(shardId = 0, address = new Address("akka", "ActorSystem1"))
      val timestamp = DateTime.now()
      Await.result(driver.createShard(op, timestamp), 5.seconds)
      val getShardResult = Await.result(driver.getShard(GetShard(op.shardId)), 5.seconds)
      getShardResult.shardId shouldEqual 0
      getShardResult.address.protocol shouldEqual "akka"
      getShardResult.address.system shouldEqual "ActorSystem1"
    }

    "fail to create a shard if it already exists" in withSessionAndDriver { (session,driver) =>
      val op = CreateShard(shardId = 0, address = new Address("akka", "ActorSystem1"))
      val timestamp = DateTime.now()
      Await.result(driver.createShard(op, timestamp), 5.seconds)
      val ex = evaluating {
        Await.result(driver.createShard(op, timestamp), 5.seconds)
      } should produce[ApiException]
      ex.failure shouldEqual Conflict
    }

    "update a shard" in withSessionAndDriver { (session,driver) =>
      val timestamp = DateTime.now()
      Await.result(driver.createShard(CreateShard(shardId = 0, address = new Address("akka", "ActorSystem1")), timestamp), 5.seconds)
      val op = UpdateShard(shardId = 0, address = new Address("akka", "ActorSystem2"), prev = new Address("akka", "ActorSystem1"))
      Await.result(driver.updateShard(op, timestamp), 5.seconds)
      val result = Await.result(driver.getShard(GetShard(op.shardId)), 5.seconds)
      result.shardId shouldEqual 0
      result.address.protocol shouldEqual "akka"
      result.address.system shouldEqual "ActorSystem2"
    }

    "fail to update a shard if the previous address doesn't match" in withSessionAndDriver { (session,driver) =>
      val timestamp = DateTime.now()
      Await.result(driver.createShard(CreateShard(shardId = 0, address = new Address("akka", "ActorSystem1")), timestamp), 5.seconds)
      val op = UpdateShard(shardId = 0, address = new Address("akka", "ActorSystem2"), prev = new Address("akka", "DoesNotMatch"))
      val ex = evaluating {
        Await.result(driver.updateShard(op, timestamp), 5.seconds)
      } should produce[ApiException]
      ex.failure shouldEqual Conflict
    }

    "fail to update a shard if it doesn't exist" in withSessionAndDriver { (session,driver) =>
      val timestamp = DateTime.now()
      val op = UpdateShard(shardId = 0, address = new Address("akka", "ActorSystem2"), prev = new Address("akka", "ActorSystem1"))
      val ex = evaluating {
        Await.result(driver.updateShard(op, timestamp), 5.seconds)
      } should produce[ApiException]
      ex.failure shouldEqual Conflict
    }

    "list shards" in withSessionAndDriver { (session,driver) =>
      val timestamp = DateTime.now()
      Await.result(driver.createShard(
        CreateShard(shardId = 0, address = new Address("akka", "ActorSystem1")),
        timestamp), 5.seconds)
      Await.result(driver.createShard(
        CreateShard(shardId = 1, address = new Address("akka", "ActorSystem2")),
        timestamp), 5.seconds)
      Await.result(driver.createShard(
        CreateShard(shardId = 2, address = new Address("akka", "ActorSystem3")),
        timestamp), 5.seconds)
      Await.result(driver.createShard(
        CreateShard(shardId = 3, address = new Address("akka", "ActorSystem4")),
        timestamp), 5.seconds)
      val op = ListShards(10, None)
      val listShardsResult = Await.result(driver.listShards(op), 5.seconds)
      listShardsResult.shards.toSet shouldEqual Set(
        Shard(0, Address("akka", "ActorSystem1")),
        Shard(1, Address("akka", "ActorSystem2")),
        Shard(2, Address("akka", "ActorSystem3")),
        Shard(3, Address("akka", "ActorSystem4"))
      )
      listShardsResult.token shouldEqual None
    }

    "page through shards" in withSessionAndDriver { (session,driver) =>
      val timestamp = DateTime.now()
      Await.result(driver.createShard(
        CreateShard(shardId = 0, address = new Address("akka", "ActorSystem1")),
        timestamp), 5.seconds)
      Await.result(driver.createShard(
        CreateShard(shardId = 1, address = new Address("akka", "ActorSystem2")),
        timestamp), 5.seconds)
      Await.result(driver.createShard(
        CreateShard(shardId = 2, address = new Address("akka", "ActorSystem3")),
        timestamp), 5.seconds)
      Await.result(driver.createShard(
        CreateShard(shardId = 3, address = new Address("akka", "ActorSystem4")),
        timestamp), 5.seconds)
      Await.result(driver.createShard(
        CreateShard(shardId = 4, address = new Address("akka", "ActorSystem5")),
        timestamp), 5.seconds)
      val listShardsResult1 = Await.result(driver.listShards(ListShards(3, None)), 5.seconds)
      listShardsResult1.shards.toSet shouldEqual Set(
        Shard(0, Address("akka", "ActorSystem1")),
        Shard(1, Address("akka", "ActorSystem2")),
        Shard(2, Address("akka", "ActorSystem3"))
      )
      listShardsResult1.token shouldEqual Some(Shard(2, Address("akka", "ActorSystem3")))
      val listShardsResult2 = Await.result(driver.listShards(ListShards(3, listShardsResult1.token)), 5.seconds)
      listShardsResult2.shards.toSet shouldEqual Set(
        Shard(3, Address("akka", "ActorSystem4")),
        Shard(4, Address("akka", "ActorSystem5"))
      )
      listShardsResult2.token shouldEqual None
    }

    "create an entity" in withSessionAndDriver { (session,driver) =>
      val op = CreateEntity(shardId = 0, entityKey = "entity1")
      val timestamp = DateTime.now()
      Await.result(driver.createEntity(op, timestamp), 5.seconds)
      val getEntityResult = Await.result(driver.getEntity(GetEntity(op.shardId, op.entityKey)), 5.seconds)
      getEntityResult.shardId shouldEqual 0
      getEntityResult.entityKey shouldEqual "entity1"
    }

    "delete an entity" in withSessionAndDriver { (session,driver) =>
      val timestamp = DateTime.now()
      Await.result(driver.createEntity(CreateEntity(shardId = 0, entityKey = "entity1"), timestamp), 5.seconds)
      val beforeDelete = Await.result(driver.getEntity(GetEntity(0, "entity1")), 5.seconds)
      beforeDelete.shardId shouldEqual 0
      beforeDelete.entityKey shouldEqual "entity1"
      val op = DeleteEntity(0, "entity1")
      val deleteEntityResult = Await.result(driver.deleteEntity(op), 5.seconds)
      val ex = evaluating {
        Await.result(driver.getEntity(GetEntity(0, "entity1")), 5.seconds)
      } should produce[ApiException]
      ex.failure shouldEqual ResourceNotFound
    }

    "list entities for a shard" in withSessionAndDriver { (session,driver) =>
      val timestamp = DateTime.now()
      Await.result(driver.createEntity(
        CreateEntity(shardId = 0, entityKey = "entity1"),
        timestamp), 5.seconds)
      Await.result(driver.createEntity(
        CreateEntity(shardId = 0, entityKey = "entity2"),
        timestamp), 5.seconds)
      Await.result(driver.createEntity(
        CreateEntity(shardId = 0, entityKey = "entity3"),
        timestamp), 5.seconds)
      val op = ListEntities(shardId = 0, limit = 10, None)
      val listEntitiesResult = Await.result(driver.listEntities(op), 5.seconds)
      listEntitiesResult.entities.toSet shouldEqual Set(
        Entity(0, "entity1"),
        Entity(0, "entity2"),
        Entity(0, "entity3")
      )
      listEntitiesResult.token shouldEqual None
    }

    "page through entities for a shard" in withSessionAndDriver { (session,driver) =>
       val timestamp = DateTime.now()
      Await.result(driver.createEntity(
        CreateEntity(shardId = 0, entityKey = "entity1"),
        timestamp), 5.seconds)
      Await.result(driver.createEntity(
        CreateEntity(shardId = 0, entityKey = "entity2"),
        timestamp), 5.seconds)
      Await.result(driver.createEntity(
        CreateEntity(shardId = 0, entityKey = "entity3"),
        timestamp), 5.seconds)
      val listEntitiesResult1 = Await.result(driver.listEntities(ListEntities(shardId = 0, limit = 2, None)), 5.seconds)
      listEntitiesResult1.entities.toSet shouldEqual Set(
        Entity(0, "entity1"),
        Entity(0, "entity2")
      )
      listEntitiesResult1.token shouldEqual Some(Entity(0, "entity2"))
      val listEntitiesResult2 = Await.result(driver.listEntities(ListEntities(shardId = 0, limit = 2, listEntitiesResult1.token)), 5.seconds)
      listEntitiesResult2.entities.toSet shouldEqual Set(Entity(0, "entity3"))
      listEntitiesResult2.token shouldEqual None
    }
  }
}
