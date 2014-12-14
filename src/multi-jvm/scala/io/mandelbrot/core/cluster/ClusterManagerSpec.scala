package io.mandelbrot.core.cluster

import akka.actor.{ActorLogging, ActorRef, Props, Actor}
import akka.testkit.{TestProbe, ImplicitSender}
import io.mandelbrot.core.{BadRequest, ResourceNotFound}
import scala.concurrent.duration._

class ClusterManagerSpecMultiJvmNode1 extends ClusterManagerSpec
class ClusterManagerSpecMultiJvmNode2 extends ClusterManagerSpec
class ClusterManagerSpecMultiJvmNode3 extends ClusterManagerSpec
class ClusterManagerSpecMultiJvmNode4 extends ClusterManagerSpec
class ClusterManagerSpecMultiJvmNode5 extends ClusterManagerSpec

class ClusterManagerSpec extends ClusterMultiNodeSpec(ClusterMultiNodeConfig) with ImplicitSender {
  import ClusterMultiNodeConfig._

  // config
  def initialParticipants = roles.size
  val totalShards = 10
  val initialWidth = 1

  val shards = ShardMap(totalShards, initialWidth)
  shards.put(0, node(node1).address)
  shards.put(1, node(node2).address)
  shards.put(2, node(node3).address)
  shards.put(3, node(node4).address)
  shards.put(4, node(node5).address)

  val clusterSettings = new ClusterSettings(enabled = true,
                                            seedNodes = Vector.empty,
                                            minNrMembers = 5,
                                            totalShards,
                                            initialWidth,
                                            CoordinatorSettings("io.mandelbrot.core.cluster.TestCoordinator", Some(shards)))
  var clusterManager = ActorRef.noSender

  "A ClusterManager" should {

    "wait for nodes to become ready" in {
      val eventStream = TestProbe()
      system.eventStream.subscribe(eventStream.ref, classOf[ClusterUp])
      val props = ClusterManager.props(clusterSettings, TestEntity.shardResolver, TestEntity.keyExtractor, TestEntity.propsCreator)
      clusterManager = system.actorOf(props, "cluster-manager")
      clusterManager ! JoinCluster(Vector(node(node1).address.toString))
      eventStream.expectMsgClass(30.seconds, classOf[ClusterUp])
      enterBarrier("cluster-up")
    }

    "create a local entity" in {
      runOn(node1) {
        clusterManager ! TestEntityCreate("test1", 0, 1)
        val reply = expectMsgClass(classOf[TestCreateReply])
        lastSender.path.address.hasLocalScope shouldEqual true
        reply.message should be(1)
      }
      enterBarrier("send-local-create")
    }

    "send a message to a local entity" in {
      runOn(node1) {
        clusterManager ! TestEntityMessage("test1", 0, 2)
        val reply = expectMsgClass(classOf[TestMessageReply])
        lastSender.path.address.hasLocalScope shouldEqual true
        reply.message should be(2)
      }
      enterBarrier("send-local-message")
    }

    "receive delivery failure sending a message to a nonexistent local entity" in {
      runOn(node1) {
        clusterManager ! TestEntityMessage("missing", 0, 3)
        val reply = expectMsgClass(classOf[EntityDeliveryFailed])
        lastSender.path.address.hasLocalScope shouldEqual true
        reply.failure.getCause shouldEqual ResourceNotFound
      }
      enterBarrier("local-delivery-failure")
    }

    "receive delivery failure sending a message of unknown type" in {
      case object UnknownMessageType
      runOn(node1) {
        clusterManager ! UnknownMessageType
        val reply = expectMsgClass(classOf[EntityDeliveryFailed])
        lastSender.path.address.hasLocalScope shouldEqual true
        reply.failure.getCause shouldEqual BadRequest
      }
      enterBarrier("unknown-message-type")
    }

    "create a remote entity" in {
      runOn(node1) {
        clusterManager ! TestEntityCreate("test2", 1, 4)
        val reply = expectMsgClass(classOf[TestCreateReply])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node2).address
        reply.message should be(4)
      }
      enterBarrier("send-remote-create")
    }

    "send a message to a remote entity" in {
      runOn(node1) {
        clusterManager ! TestEntityMessage("test2", 1, 5)
        val reply = expectMsgClass(classOf[TestMessageReply])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node2).address
        reply.message should be(5)
      }
      enterBarrier("send-remote-message")
    }

    "receive delivery failure sending a message to a nonexistent remote entity" in {
      runOn(node1) {
        clusterManager ! TestEntityMessage("missing", 1, 6)
        val reply = expectMsgClass(classOf[EntityDeliveryFailed])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node2).address
        reply.failure.getCause shouldEqual ResourceNotFound
      }
      enterBarrier("remote-delivery-failure")
    }

    "send message which is redirected" in {
      runOn(node1) {
        shards.put(6, node(node2).address)
        enterBarrier("setup-successful-redirect")
      }
      runOn(node2) {
        shards.put(6, node(node3).address)
        enterBarrier("setup-successful-redirect")
      }
      runOn(node3) {
        shards.put(6, node(node3).address)
        enterBarrier("setup-successful-redirect")
      }
      runOn(node4, node5) {
        enterBarrier("setup-successful-redirect")
      }
      runOn(node1) {
        clusterManager ! TestEntityCreate("redirect-succeeds", 6, 7)
        val reply = expectMsgClass(classOf[TestCreateReply])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node3).address
        reply.message should be(7)
      }
      enterBarrier("finish-successful-redirect")
    }

    "receive delivery failure sending a message which redirects too many times" in {
      runOn(node1) {
        shards.put(7, node(node2).address)
        enterBarrier("setup-redirect-failure")
      }
      runOn(node2) {
        shards.put(7, node(node3).address)
        enterBarrier("setup-redirect-failure")
      }
      runOn(node3) {
        shards.put(7, node(node4).address)
        enterBarrier("setup-redirect-failure")
      }
      runOn(node4) {
        shards.put(7, node(node5).address)
        enterBarrier("setup-redirect-failure")
      }
      runOn(node5) {
        shards.put(7, node(node5).address)
        enterBarrier("setup-redirect-failure")
      }
      runOn(node1) {
        clusterManager ! TestEntityCreate("redirect-fails", 7, 8)
        val reply = expectMsgClass(classOf[EntityDeliveryFailed])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node4).address
        reply.failure.getCause shouldEqual ResourceNotFound
      }
      enterBarrier("finish-redirect-failure")
    }
  }
}

case class TestEntityCreate(key: String, shard: Int, message: Any)
case class TestCreateReply(message: Any)

case class TestEntityMessage(key: String, shard: Int, message: Any)
case class TestMessageReply(message: Any)

class TestEntity extends Actor with ActorLogging {
  log.debug("initialized TestEntity")
  def receive = {
    case m @ TestEntityCreate(key, shard, message) =>
      log.debug("received {}", m)
      sender() ! TestCreateReply(message)
    case m @ TestEntityMessage(key, shard, message) =>
      log.debug("received {}", m)
      sender() ! TestMessageReply(message)
  }
}

object TestEntity {
  def props() = Props(classOf[TestEntity])

  val shardResolver: EntityFunctions.ShardResolver = {
    case m: TestEntityCreate => m.shard
    case m: TestEntityMessage => m.shard
  }
  val keyExtractor: EntityFunctions.KeyExtractor = {
    case m: TestEntityCreate => m.key
    case m: TestEntityMessage => m.key
  }
  val propsCreator: EntityFunctions.PropsCreator = {
    case m: TestEntityCreate => TestEntity.props()
  }
}