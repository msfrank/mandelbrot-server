package io.mandelbrot.core.entity

import akka.actor.{ActorSystem, Props}
import akka.testkit.{TestKit, ImplicitSender}
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import org.scalatest.matchers.ShouldMatchers

import io.mandelbrot.core.{AkkaConfig, ProxyForwarder, ResourceNotFound, ServiceOperation}
import io.mandelbrot.core.ConfigConversions._

class StandaloneEntityManagerSpec(_system: ActorSystem) extends TestKit(_system) with WordSpecLike with ImplicitSender with ShouldMatchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("StandaloneEntityManagerSpec", AkkaConfig))

  // shutdown the actor system
  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  // config
  val totalShards = 4
  val deliveryAttempts = 3

  val shards = ShardMap(totalShards)
  shards.assign(0, ShardManager.StandaloneAddress)
  shards.assign(1, ShardManager.StandaloneAddress)
  shards.assign(2, ShardManager.StandaloneAddress)
  shards.assign(3, ShardManager.StandaloneAddress)

  val initialEntities = Vector.empty[Entity]

  val coordinatorSettings = TestCoordinatorSettings(shards, initialEntities, ShardManager.StandaloneAddress, ShardManager.StandaloneAddress)

  val clusterSettings = new ClusterSettings(enabled = false,
                                            seedNodes = Vector.empty,
                                            minNrMembers = 0,
                                            totalShards,
                                            deliveryAttempts,
                                            CoordinatorSettings("io.mandelbrot.core.entity.TestCoordinator", Some(coordinatorSettings)))

  val props = Props(classOf[StandaloneEntityManager], clusterSettings, TestEntity.propsCreator)
  val entityManager = system.actorOf(ProxyForwarder.props(props, self, classOf[EntityServiceOperation]), "entity-manager")

  def entityEnvelope(op: ServiceOperation): EntityEnvelope = {
    val shardKey = TestEntity.shardResolver(op)
    val entityKey = TestEntity.keyExtractor(op)
    EntityEnvelope(self, op, shardKey, entityKey, clusterSettings.deliveryAttempts, clusterSettings.deliveryAttempts)
  }

  "A StandaloneEntityManager" should {

    "create a local entity" in {
      entityManager ! entityEnvelope(TestEntityCreate("test1", 0, 1))
      val reply = expectMsgClass(classOf[TestCreateReply])
      lastSender.path.address.hasLocalScope
      lastSender.path.address.hasLocalScope shouldEqual true
      reply.message should be(1)
    }

    "send a message to a local entity" in {
        entityManager ! entityEnvelope(TestEntityMessage("test1", 0, 2))
        val reply = expectMsgClass(classOf[TestMessageReply])
        lastSender.path.address.hasLocalScope shouldEqual true
        reply.message should be(2)
    }

    "receive delivery failure sending a message to a nonexistent local entity" in {
        entityManager ! entityEnvelope(TestEntityMessage("missing", 0, 3))
        val reply = expectMsgClass(classOf[EntityDeliveryFailed])
        lastSender.path.address.hasLocalScope shouldEqual true
        reply.failure.getCause shouldEqual ResourceNotFound
    }
  }
}
