package io.mandelbrot.core.entity

import akka.actor.ActorRef
import akka.contrib.pattern.DistributedPubSubMediator.SendToAll
import akka.testkit.{TestActorRef, ImplicitSender}
import org.scalatest.Inside._
import scala.concurrent.duration._

import io.mandelbrot.core.{ApiException, ServiceOperation, ResourceNotFound}

class ShardManagerSpecMultiJvmNode1 extends ShardManagerSpec
class ShardManagerSpecMultiJvmNode2 extends ShardManagerSpec
class ShardManagerSpecMultiJvmNode3 extends ShardManagerSpec
class ShardManagerSpecMultiJvmNode4 extends ShardManagerSpec
class ShardManagerSpecMultiJvmNode5 extends ShardManagerSpec

class ShardManagerSpec extends MultiNodeSpec(ClusterMultiNodeConfig) with ImplicitSender {
  import ClusterMultiNodeConfig._

  // config
  def initialParticipants = roles.size
  val totalShards = 10

  val shardMap = ShardMap(totalShards)
  shardMap.assign(0, node(node1).address)
  shardMap.assign(1, node(node2).address)
  shardMap.assign(2, node(node3).address)
  shardMap.assign(3, node(node4).address)
  shardMap.assign(4, node(node5).address)

  val initialEntities = Vector.empty[Entity]

  val coordinatorSettings = TestEntityCoordinatorSettings(shardMap, initialEntities, node(node1).address, myAddress)
  val coordinator = system.actorOf(TestEntityCoordinator.props(coordinatorSettings), "coordinator")
  val shardManager = TestActorRef[ShardManager](ShardManager.props(coordinator, TestEntity.propsCreator, myAddress, totalShards, self),
    "entities")

  def entityEnvelope(sender: ActorRef, op: ServiceOperation, attempts: Int): EntityEnvelope = {
    val shardKey = TestEntity.shardResolver(op)
    val entityKey = TestEntity.keyExtractor(op)
    EntityEnvelope(sender, op, shardKey, entityKey, attempts, attempts)
  }

  "A ShardManager" should {

    "create a local entity" in {
      enterBarrier("")
      runOn(node1) {
        shardManager ! entityEnvelope(self, TestEntityCreate("test1", 0, 1), attempts = 3)
        val reply = expectMsgClass(classOf[TestCreateReply])
        lastSender.path.address.hasLocalScope shouldEqual true
        reply.message should be(1)
      }
      enterBarrier("send-local-create")
    }

    "send a message to a local entity" in {
      runOn(node1) {
        shardManager ! entityEnvelope(self, TestEntityMessage("test1", 0, 2), attempts = 3)
        val reply = expectMsgClass(classOf[TestMessageReply])
        lastSender.path.address.hasLocalScope shouldEqual true
        reply.message should be(2)
      }
      enterBarrier("send-local-message")
    }

    "receive delivery failure sending a message to a nonexistent local entity" in {
      runOn(node1) {
        shardManager ! entityEnvelope(self, TestEntityMessage("missing", 0, 3), attempts = 3)
        val reply = expectMsgClass(classOf[EntityDeliveryFailed])
        lastSender.path.address.hasLocalScope shouldEqual true
        reply.failure shouldEqual ApiException(ResourceNotFound)
      }
      enterBarrier("local-delivery-failure")
    }

    "create a remote entity" in {
      runOn(node1) {
        shardManager ! entityEnvelope(self, TestEntityCreate("test2", 1, 4), attempts = 3)
        val reply = expectMsgClass(classOf[TestCreateReply])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node2).address
        reply.message should be(4)
      }
      enterBarrier("send-remote-create")
    }

    "send a message to a remote entity" in {
      runOn(node1) {
        shardManager ! entityEnvelope(self, TestEntityMessage("test2", 1, 5), attempts = 3)
        val reply = expectMsgClass(classOf[TestMessageReply])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node2).address
        reply.message should be(5)
      }
      enterBarrier("send-remote-message")
    }

    "receive delivery failure sending a message to a nonexistent remote entity" in {
      runOn(node1) {
        shardManager ! entityEnvelope(self, TestEntityMessage("missing", 1, 6), attempts = 3)
        val reply = expectMsgClass(classOf[EntityDeliveryFailed])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node2).address
        reply.failure shouldEqual ApiException(ResourceNotFound)
      }
      enterBarrier("remote-delivery-failure")
    }

    "send message which is redirected" in {
      runOn(node1) {
        shardManager.underlyingActor.shardMap.assign(2, node(node2).address)
        enterBarrier("setup-successful-redirect")
        shardManager ! entityEnvelope(self, TestEntityCreate("redirect-succeeds", 2, 7), attempts = 3)
        val reply = expectMsgClass(classOf[TestCreateReply])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node3).address
        reply.message should be(7)
      }
      runOn(node2) {
        enterBarrier("setup-successful-redirect")
        inside(expectMsgClass(7.seconds, classOf[SendToAll])) {
          case SendToAll(_, StaleShardSet(staleShards), _) =>
            staleShards shouldEqual Set(StaleShard(2, node(node2).address))
        }
      }
      runOn(node3) {
        enterBarrier("setup-successful-redirect")
      }
      runOn(node4) {
        enterBarrier("setup-successful-redirect")
      }
      runOn(node5) {
        enterBarrier("setup-successful-redirect")
      }
      enterBarrier("finish-successful-redirect")
    }

    "receive delivery failure sending a message which redirects too many times" in {
      val a = Seq(node(node1).address, node(node2).address, node(node3).address, node(node4).address, node(node5).address)
      runOn(node1) {
        shardManager.underlyingActor.shardMap.assign(4, node(node2).address)
        enterBarrier("setup-redirect-failure")
        shardManager ! entityEnvelope(self, TestEntityCreate("redirect-fails", 4, 8), attempts = 3)
        val reply = expectMsgClass(classOf[EntityDeliveryFailed])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node4).address
        reply.failure shouldEqual ApiException(ResourceNotFound)
      }
      runOn(node2) {
        shardManager.underlyingActor.shardMap.assign(4, node(node3).address)
        enterBarrier("setup-redirect-failure")
        inside(expectMsgClass(7.seconds, classOf[SendToAll])) {
          case SendToAll(_, StaleShardSet(staleShards), _) =>
            staleShards shouldEqual Set(StaleShard(4, node(node2).address))
        }
      }
      runOn(node3) {
        shardManager.underlyingActor.shardMap.assign(4, node(node4).address)
        enterBarrier("setup-redirect-failure")
        inside(expectMsgClass(7.seconds, classOf[SendToAll])) {
          case SendToAll(_, StaleShardSet(staleShards), _) =>
            staleShards shouldEqual Set(StaleShard(4, node(node3).address))
        }
      }
      runOn(node4) {
        enterBarrier("setup-redirect-failure")
        inside(expectMsgClass(7.seconds, classOf[SendToAll])) {
          case SendToAll(_, StaleShardSet(staleShards), _) =>
            staleShards shouldEqual Set(StaleShard(4, node(node4).address))
        }
      }
      runOn(node5) {
        enterBarrier("setup-redirect-failure")
      }
      enterBarrier("finish-redirect-failure")
    }
  }
}
