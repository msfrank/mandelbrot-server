package io.mandelbrot.core.cluster

import akka.actor.{ActorLogging, ActorRef, Props, Actor}
import akka.testkit.{TestProbe, ImplicitSender}
import io.mandelbrot.core.{BadRequest, ApiException, ResourceNotFound}
import scala.concurrent.duration._

class ClusterManagerSpecMultiJvmNode1 extends ClusterManagerSpec
class ClusterManagerSpecMultiJvmNode2 extends ClusterManagerSpec
class ClusterManagerSpecMultiJvmNode3 extends ClusterManagerSpec
class ClusterManagerSpecMultiJvmNode4 extends ClusterManagerSpec
class ClusterManagerSpecMultiJvmNode5 extends ClusterManagerSpec

class ClusterManagerSpec extends ClusterMultiNodeSpec(ClusterMultiNodeConfig) with ImplicitSender {
  import ClusterMultiNodeConfig._

  def initialParticipants = roles.size

  val shards = ShardRing()
  shards.put(1, 1, node(node1).address)
  shards.put(2, 1, node(node2).address)
  shards.put(3, 1, node(node3).address)
  shards.put(4, 1, node(node4).address)
  shards.put(5, 1, node(node5).address)

  val clusterSettings = new ClusterSettings(enabled = true,
                                            seedNodes = Vector.empty,
                                            minNrMembers = 5,
                                            shardCount = 100,
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
        clusterManager ! TestEntityCreate("test1", 1, 1)
        val reply = expectMsgClass(classOf[TestCreateReply])
        lastSender.path.address.hasLocalScope shouldEqual true
        reply.message should be(1)
      }
      enterBarrier("send-local-create")
    }

    "send a message to a local entity" in {
      runOn(node1) {
        clusterManager ! TestEntityMessage("test1", 1, 2)
        val reply = expectMsgClass(classOf[TestMessageReply])
        lastSender.path.address.hasLocalScope shouldEqual true
        reply.message should be(2)
      }
      enterBarrier("send-local-message")
    }

    "receive delivery failure sending a message to a nonexistent local entity" in {
      runOn(node1) {
        clusterManager ! TestEntityMessage("missing", 1, 3)
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
        clusterManager ! TestEntityCreate("test2", 2, 4)
        val reply = expectMsgClass(classOf[TestCreateReply])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node2).address
        reply.message should be(4)
      }
      enterBarrier("send-remote-create")
    }

    "send a message to a remote entity" in {
      runOn(node1) {
        clusterManager ! TestEntityMessage("test2", 2, 5)
        val reply = expectMsgClass(classOf[TestMessageReply])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node2).address
        reply.message should be(5)
      }
      enterBarrier("send-remote-message")
    }

    "receive delivery failure sending a message to a nonexistent remote entity" in {
      runOn(node1) {
        clusterManager ! TestEntityMessage("missing", 2, 6)
        val reply = expectMsgClass(classOf[EntityDeliveryFailed])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node2).address
        reply.failure.getCause shouldEqual ResourceNotFound
      }
      enterBarrier("remote-delivery-failure")
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