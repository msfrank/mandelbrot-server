package io.mandelbrot.persistence.cassandra

import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.{Address, ActorSystem}
import com.datastax.driver.core.Session
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import org.scalatest.matchers.ShouldMatchers
import org.joda.time.DateTime
import scala.concurrent.duration._
import scala.concurrent.Await

import io.mandelbrot.core.{Conflict, ApiException, AkkaConfig}
import io.mandelbrot.core.ConfigConversions._
import io.mandelbrot.core.entity._
import io.mandelbrot.persistence.cassandra.CassandraCoordinator.CassandraCoordinatorSettings

class ShardsDALSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ShardsDALSpec", AkkaConfig ++ CassandraConfig))

  override def afterAll(): Unit = {
    Cassandra(system).dropKeyspace()
  }

  val settings = CassandraCoordinatorSettings()

  "A ShardsDAL" should {

    "create the shards table during initialization" in {
      val session = Cassandra(system).getSession
      val dal = new ShardsDAL(settings, session)(system.dispatcher)
      val keyspaceName = Cassandra(system).keyspaceName
      val keyspaceMeta = session.getCluster.getMetadata.getKeyspace(keyspaceName)
      val table = keyspaceMeta.getTable(dal.tableName)
      table should not be null
      table.getName shouldEqual dal.tableName
    }
  }

  "A ShardsDAL" should {

    var _dal: ShardsDAL = null

    def withSessionAndDAL(testCode: (Session, ShardsDAL) => Any) = {
      import system.dispatcher
      val session = Cassandra(system).getSession
      if (_dal == null)
        _dal = new ShardsDAL(settings, session)(system.dispatcher)
      Await.result(_dal.flushShards(), 5.seconds)
      testCode(session, _dal)
    }

    "create a shard" in withSessionAndDAL { (session, dal) =>
      val op = CreateShard(shardId = 0, address = new Address("akka", "ActorSystem1"))
      val timestamp = DateTime.now()
      Await.result(dal.createShard(op, timestamp), 5.seconds)
      val getShardResult = Await.result(dal.getShard(GetShard(op.shardId)), 5.seconds)
      getShardResult.shardId shouldEqual 0
      getShardResult.address.protocol shouldEqual "akka"
      getShardResult.address.system shouldEqual "ActorSystem1"
    }

    "fail to create a shard if it already exists" in withSessionAndDAL { (session, dal) =>
      val op = CreateShard(shardId = 0, address = new Address("akka", "ActorSystem1"))
      val timestamp = DateTime.now()
      Await.result(dal.createShard(op, timestamp), 5.seconds)
      val ex = evaluating {
        Await.result(dal.createShard(op, timestamp), 5.seconds)
      } should produce[ApiException]
      ex.failure shouldEqual Conflict
    }

    "update a shard" in withSessionAndDAL { (session, dal) =>
      val timestamp = DateTime.now()
      Await.result(dal.createShard(CreateShard(shardId = 0, address = new Address("akka", "ActorSystem1")), timestamp), 5.seconds)
      val op = UpdateShard(shardId = 0, address = new Address("akka", "ActorSystem2"), prev = new Address("akka", "ActorSystem1"))
      Await.result(dal.updateShard(op, timestamp), 5.seconds)
      val result = Await.result(dal.getShard(GetShard(op.shardId)), 5.seconds)
      result.shardId shouldEqual 0
      result.address.protocol shouldEqual "akka"
      result.address.system shouldEqual "ActorSystem2"
    }

    "fail to update a shard if the previous address doesn't match" in withSessionAndDAL { (session, dal) =>
      val timestamp = DateTime.now()
      Await.result(dal.createShard(CreateShard(shardId = 0, address = new Address("akka", "ActorSystem1")), timestamp), 5.seconds)
      val op = UpdateShard(shardId = 0, address = new Address("akka", "ActorSystem2"), prev = new Address("akka", "DoesNotMatch"))
      val ex = evaluating {
        Await.result(dal.updateShard(op, timestamp), 5.seconds)
      } should produce[ApiException]
      ex.failure shouldEqual Conflict
    }

    "fail to update a shard if it doesn't exist" in withSessionAndDAL { (session, dal) =>
      val timestamp = DateTime.now()
      val op = UpdateShard(shardId = 0, address = new Address("akka", "ActorSystem2"), prev = new Address("akka", "ActorSystem1"))
      val ex = evaluating {
        Await.result(dal.updateShard(op, timestamp), 5.seconds)
      } should produce[ApiException]
      ex.failure shouldEqual Conflict
    }

    "list shards" in withSessionAndDAL { (session, dal) =>
      val timestamp = DateTime.now()
      Await.result(dal.createShard(
        CreateShard(shardId = 0, address = new Address("akka", "ActorSystem1")),
        timestamp), 5.seconds)
      Await.result(dal.createShard(
        CreateShard(shardId = 1, address = new Address("akka", "ActorSystem2")),
        timestamp), 5.seconds)
      Await.result(dal.createShard(
        CreateShard(shardId = 2, address = new Address("akka", "ActorSystem3")),
        timestamp), 5.seconds)
      Await.result(dal.createShard(
        CreateShard(shardId = 3, address = new Address("akka", "ActorSystem4")),
        timestamp), 5.seconds)
      val op = ListShards(10, None)
      val listShardsResult = Await.result(dal.listShards(op), 5.seconds)
      listShardsResult.shards.toSet shouldEqual Set(
        Shard(0, Address("akka", "ActorSystem1")),
        Shard(1, Address("akka", "ActorSystem2")),
        Shard(2, Address("akka", "ActorSystem3")),
        Shard(3, Address("akka", "ActorSystem4"))
      )
      listShardsResult.token shouldEqual None
    }

    "page through shards" in withSessionAndDAL { (session, dal) =>
      val timestamp = DateTime.now()
      Await.result(dal.createShard(
        CreateShard(shardId = 0, address = new Address("akka", "ActorSystem1")),
        timestamp), 5.seconds)
      Await.result(dal.createShard(
        CreateShard(shardId = 1, address = new Address("akka", "ActorSystem2")),
        timestamp), 5.seconds)
      Await.result(dal.createShard(
        CreateShard(shardId = 2, address = new Address("akka", "ActorSystem3")),
        timestamp), 5.seconds)
      Await.result(dal.createShard(
        CreateShard(shardId = 3, address = new Address("akka", "ActorSystem4")),
        timestamp), 5.seconds)
      Await.result(dal.createShard(
        CreateShard(shardId = 4, address = new Address("akka", "ActorSystem5")),
        timestamp), 5.seconds)
      val listShardsResult1 = Await.result(dal.listShards(ListShards(3, None)), 5.seconds)
      listShardsResult1.shards.toSet shouldEqual Set(
        Shard(0, Address("akka", "ActorSystem1")),
        Shard(1, Address("akka", "ActorSystem2")),
        Shard(2, Address("akka", "ActorSystem3"))
      )
      listShardsResult1.token shouldEqual Some(Shard(2, Address("akka", "ActorSystem3")))
      val listShardsResult2 = Await.result(dal.listShards(ListShards(3, listShardsResult1.token)), 5.seconds)
      listShardsResult2.shards.toSet shouldEqual Set(
        Shard(3, Address("akka", "ActorSystem4")),
        Shard(4, Address("akka", "ActorSystem5"))
      )
      listShardsResult2.token shouldEqual None
    }
  }
}
