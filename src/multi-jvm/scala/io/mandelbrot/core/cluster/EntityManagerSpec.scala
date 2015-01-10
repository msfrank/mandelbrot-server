package io.mandelbrot.core.cluster

import akka.actor.ActorRef
import akka.testkit.{TestActorRef, TestProbe, ImplicitSender}
import scala.concurrent.duration._

import io.mandelbrot.core.{BadRequest, ResourceNotFound}

class EntityManagerSpecMultiJvmNode1 extends EntityManagerSpec
class EntityManagerSpecMultiJvmNode2 extends EntityManagerSpec
class EntityManagerSpecMultiJvmNode3 extends EntityManagerSpec
class EntityManagerSpecMultiJvmNode4 extends EntityManagerSpec
class EntityManagerSpecMultiJvmNode5 extends EntityManagerSpec

class EntityManagerSpec extends MultiNodeSpec(ClusterMultiNodeConfig) with ImplicitSender {
  import ClusterMultiNodeConfig._

  // config
  def initialParticipants = roles.size
  val totalShards = 10
  val initialWidth = 1

  val shardMap = ShardMap(totalShards, initialWidth)
  shardMap.assign(0, node(node1).address)
  shardMap.assign(1, node(node2).address)
  shardMap.assign(2, node(node3).address)
  shardMap.assign(3, node(node4).address)
  shardMap.assign(4, node(node5).address)

  val coordinatorSettings = TestCoordinatorSettings(shardMap, node(node1).address, myAddress)
  val coordinator = system.actorOf(TestCoordinator.props(coordinatorSettings), "coordinator")
  val entityManager = TestActorRef[EntityManager](EntityManager.props(coordinator,
    TestEntity.shardResolver, TestEntity.keyExtractor, TestEntity.propsCreator, myAddress, totalShards, initialWidth),
    "entities")

  "An EntityManager" should {

    "create a local entity" in {
      enterBarrier("")
      runOn(node1) {
        entityManager ! EntityEnvelope(self, TestEntityCreate("test1", 0, 1), attempts = 3)
        val reply = expectMsgClass(classOf[TestCreateReply])
        lastSender.path.address.hasLocalScope shouldEqual true
        reply.message should be(1)
      }
      enterBarrier("send-local-create")
    }

    "send a message to a local entity" in {
      runOn(node1) {
        entityManager ! EntityEnvelope(self, TestEntityMessage("test1", 0, 2), attempts = 3)
        val reply = expectMsgClass(classOf[TestMessageReply])
        lastSender.path.address.hasLocalScope shouldEqual true
        reply.message should be(2)
      }
      enterBarrier("send-local-message")
    }

    "receive delivery failure sending a message to a nonexistent local entity" in {
      runOn(node1) {
        entityManager ! EntityEnvelope(self, TestEntityMessage("missing", 0, 3), attempts = 3)
        val reply = expectMsgClass(classOf[EntityDeliveryFailed])
        lastSender.path.address.hasLocalScope shouldEqual true
        reply.failure.getCause shouldEqual ResourceNotFound
      }
      enterBarrier("local-delivery-failure")
    }

    "receive delivery failure sending a message of unknown type" in {
      case object UnknownMessageType
      runOn(node1) {
        entityManager ! EntityEnvelope(self, UnknownMessageType, attempts = 3)
        val reply = expectMsgClass(classOf[EntityDeliveryFailed])
        lastSender.path.address.hasLocalScope shouldEqual true
        reply.failure.getCause shouldEqual BadRequest
      }
      enterBarrier("unknown-message-type")
    }

    "create a remote entity" in {
      runOn(node1) {
        entityManager ! EntityEnvelope(self, TestEntityCreate("test2", 1, 4), attempts = 3)
        val reply = expectMsgClass(classOf[TestCreateReply])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node2).address
        reply.message should be(4)
      }
      enterBarrier("send-remote-create")
    }

    "send a message to a remote entity" in {
      runOn(node1) {
        entityManager ! EntityEnvelope(self, TestEntityMessage("test2", 1, 5), attempts = 3)
        val reply = expectMsgClass(classOf[TestMessageReply])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node2).address
        reply.message should be(5)
      }
      enterBarrier("send-remote-message")
    }

    "receive delivery failure sending a message to a nonexistent remote entity" in {
      runOn(node1) {
        entityManager ! EntityEnvelope(self, TestEntityMessage("missing", 1, 6), attempts = 3)
        val reply = expectMsgClass(classOf[EntityDeliveryFailed])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node2).address
        reply.failure.getCause shouldEqual ResourceNotFound
      }
      enterBarrier("remote-delivery-failure")
    }

    "send message which is redirected" in {
      runOn(node1) {
        entityManager.underlyingActor.shardMap.assign(6, node(node2).address)
        enterBarrier("setup-successful-redirect")
      }
      runOn(node2) {
        entityManager.underlyingActor.shardMap.assign(6, node(node3).address)
        enterBarrier("setup-successful-redirect")
      }
      runOn(node3) {
        entityManager ! PrepareShard(6)
        expectMsgClass(classOf[PrepareShardResult])
        entityManager ! RecoverShard(6)
        expectMsgClass(classOf[RecoverShardResult])
        enterBarrier("setup-successful-redirect")
      }
      runOn(node4, node5) {
        enterBarrier("setup-successful-redirect")
      }
      runOn(node1) {
        entityManager ! EntityEnvelope(self, TestEntityCreate("redirect-succeeds", 6, 7), attempts = 3)
        val reply = expectMsgClass(classOf[TestCreateReply])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node3).address
        reply.message should be(7)
      }
      enterBarrier("finish-successful-redirect")
    }

    "receive delivery failure sending a message which redirects too many times" in {
      runOn(node1) {
        entityManager.underlyingActor.shardMap.assign(7, node(node2).address)
        enterBarrier("setup-redirect-failure")
      }
      runOn(node2) {
        entityManager.underlyingActor.shardMap.assign(7, node(node3).address)
        enterBarrier("setup-redirect-failure")
      }
      runOn(node3) {
        entityManager.underlyingActor.shardMap.assign(7, node(node4).address)
        enterBarrier("setup-redirect-failure")
      }
      runOn(node4) {
        entityManager.underlyingActor.shardMap.assign(7, node(node5).address)
        enterBarrier("setup-redirect-failure")
      }
      runOn(node5) {
        entityManager.underlyingActor.shardMap.assign(7, node(node5).address)
        enterBarrier("setup-redirect-failure")
      }
      runOn(node1) {
        entityManager ! EntityEnvelope(self, TestEntityCreate("redirect-fails", 7, 8), attempts = 3)
        val reply = expectMsgClass(classOf[EntityDeliveryFailed])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node4).address
        reply.failure.getCause shouldEqual ResourceNotFound
      }
      enterBarrier("finish-redirect-failure")
    }
  }
}
