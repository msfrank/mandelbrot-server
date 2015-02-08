package io.mandelbrot.core.entity

import akka.actor.{Props, ActorRef}
import akka.testkit.{TestProbe, ImplicitSender}
import scala.concurrent.duration._

import io.mandelbrot.core.{ServiceOperation, ProxyForwarder, BadRequest, ResourceNotFound}

class ClusterEntityManagerSpecMultiJvmNode1 extends ClusterEntityManagerSpec
class ClusterEntityManagerSpecMultiJvmNode2 extends ClusterEntityManagerSpec
class ClusterEntityManagerSpecMultiJvmNode3 extends ClusterEntityManagerSpec
class ClusterEntityManagerSpecMultiJvmNode4 extends ClusterEntityManagerSpec
class ClusterEntityManagerSpecMultiJvmNode5 extends ClusterEntityManagerSpec

class ClusterEntityManagerSpec extends MultiNodeSpec(ClusterMultiNodeConfig) with ImplicitSender {
  import ClusterMultiNodeConfig._

  // config
  def initialParticipants = roles.size
  val totalShards = 10
  val deliveryAttempts = 3

  val shards = ShardMap(totalShards)
  shards.assign(0, node(node1).address)
  shards.assign(1, node(node2).address)
  shards.assign(2, node(node3).address)
  shards.assign(3, node(node4).address)
  shards.assign(4, node(node5).address)

  val initialEntities = Vector.empty[Entity]

  val coordinatorSettings = TestCoordinatorSettings(shards, initialEntities, node(node1).address, myAddress)

  val clusterSettings = new ClusterSettings(enabled = true,
                                            seedNodes = Vector.empty,
                                            minNrMembers = 5,
                                            totalShards,
                                            deliveryAttempts,
                                            CoordinatorSettings("io.mandelbrot.core.entity.TestCoordinator", Some(coordinatorSettings)))
  var entityManager = ActorRef.noSender

  def entityEnvelope(op: ServiceOperation): EntityEnvelope = {
    val shardKey = TestEntity.shardResolver(op)
    val entityKey = TestEntity.keyExtractor(op)
    EntityEnvelope(self, op, shardKey, entityKey, clusterSettings.deliveryAttempts, clusterSettings.deliveryAttempts)
  }

  "A ClusterEntityManager" should {

    "wait for nodes to become ready" in {
      val eventStream = TestProbe()
      system.eventStream.subscribe(eventStream.ref, classOf[ClusterUp])
      val props = Props(classOf[ClusterEntityManager], clusterSettings, TestEntity.propsCreator)
      entityManager = system.actorOf(ProxyForwarder.props(props, self, classOf[EntityServiceOperation]), "cluster-manager")
      entityManager ! JoinCluster(Vector(node(node1).address.toString))
      eventStream.expectMsgClass(30.seconds, classOf[ClusterUp])
      enterBarrier("cluster-up")
    }

    "create a local entity" in {
      runOn(node1) {
        entityManager ! entityEnvelope(TestEntityCreate("test1", 0, 1))
        val reply = expectMsgClass(classOf[TestCreateReply])
        lastSender.path.address.hasLocalScope shouldEqual true
        reply.message should be(1)
      }
      enterBarrier("send-local-create")
    }

    "send a message to a local entity" in {
      runOn(node1) {
        entityManager ! entityEnvelope(TestEntityMessage("test1", 0, 2))
        val reply = expectMsgClass(classOf[TestMessageReply])
        lastSender.path.address.hasLocalScope shouldEqual true
        reply.message should be(2)
      }
      enterBarrier("send-local-message")
    }

    "receive delivery failure sending a message to a nonexistent local entity" in {
      runOn(node1) {
        entityManager ! entityEnvelope(TestEntityMessage("missing", 0, 3))
        val reply = expectMsgClass(classOf[EntityDeliveryFailed])
        lastSender.path.address.hasLocalScope shouldEqual true
        reply.failure.getCause shouldEqual ResourceNotFound
      }
      enterBarrier("local-delivery-failure")
    }

    "create a remote entity" in {
      runOn(node1) {
        entityManager ! entityEnvelope(TestEntityCreate("test2", 1, 4))
        val reply = expectMsgClass(classOf[TestCreateReply])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node2).address
        reply.message should be(4)
      }
      enterBarrier("send-remote-create")
    }

    "send a message to a remote entity" in {
      runOn(node1) {
        entityManager ! entityEnvelope(TestEntityMessage("test2", 1, 5))
        val reply = expectMsgClass(classOf[TestMessageReply])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node2).address
        reply.message should be(5)
      }
      enterBarrier("send-remote-message")
    }

    "receive delivery failure sending a message to a nonexistent remote entity" in {
      runOn(node1) {
        entityManager ! entityEnvelope(TestEntityMessage("missing", 1, 6))
        val reply = expectMsgClass(classOf[EntityDeliveryFailed])
        lastSender.path.address.hasLocalScope shouldEqual false
        lastSender.path.address shouldEqual node(node2).address
        reply.failure.getCause shouldEqual ResourceNotFound
      }
      enterBarrier("remote-delivery-failure")
    }
  }
}
