package io.mandelbrot.core.entity

import akka.testkit.{TestProbe, ImplicitSender}

class BalancerTaskSpecMultiJvmNode1 extends BalancerTaskSpec
class BalancerTaskSpecMultiJvmNode2 extends BalancerTaskSpec
class BalancerTaskSpecMultiJvmNode3 extends BalancerTaskSpec
class BalancerTaskSpecMultiJvmNode4 extends BalancerTaskSpec
class BalancerTaskSpecMultiJvmNode5 extends BalancerTaskSpec

class BalancerTaskSpec extends MultiNodeSpec(RemoteMultiNodeConfig) with ImplicitSender {
  import ClusterMultiNodeConfig._

  def initialParticipants = roles.size

  "A BalancerTask" should {

    "perform no operations if shard map is balanced" in {

      val totalShards = 5

      val shards = ShardMap(totalShards)
      shards.assign(0, node(node1).address)
      shards.assign(1, node(node2).address)
      shards.assign(2, node(node3).address)
      shards.assign(3, node(node4).address)
      shards.assign(4, node(node5).address)

      val initialEntities = Vector.empty[Entity]

      val coordinatorSettings = TestEntityCoordinatorSettings(shards, initialEntities,
        node(node1).address, myAddress)
      val coordinator = system.actorOf(TestEntityCoordinator.props(coordinatorSettings), "coordinator_1")
      val entityManager = system.actorOf(ShardManager.props(coordinator, TestEntity.propsCreator,
        TestEntity.entityReviver, myAddress, totalShards, self), "entities_1")

      enterBarrier("start-balancer-no-ops")

      runOn(node1) {
        val nodes = Map(
          node(node1).address -> node(node1) / entityManager.path.elements,
          node(node2).address -> node(node2) / entityManager.path.elements,
          node(node3).address -> node(node3) / entityManager.path.elements,
          node(node4).address -> node(node4) / entityManager.path.elements,
          node(node5).address -> node(node5) / entityManager.path.elements
        )
        val monitor = TestProbe()
        system.actorOf(BalancerTask.props(coordinator, monitor.ref, nodes, totalShards), "balancer_1")
        val result = monitor.expectMsgClass(classOf[BalancerComplete])

        shards.get(0) shouldEqual AssignedShardEntry(0, node(node1).address)
        shards.get(1) shouldEqual AssignedShardEntry(1, node(node2).address)
        shards.get(2) shouldEqual AssignedShardEntry(2, node(node3).address)
        shards.get(3) shouldEqual AssignedShardEntry(3, node(node4).address)
        shards.get(4) shouldEqual AssignedShardEntry(4, node(node5).address)
      }

      enterBarrier("end-balancer-no-ops")
    }

    "repair the shard map if a shard is missing" in {

      val totalShards = 5
      val initialWidth = 1
      val shards = ShardMap(totalShards)
      shards.assign(0, node(node1).address)
      shards.assign(2, node(node3).address)
      shards.assign(3, node(node4).address)
      shards.assign(4, node(node5).address)

      val initialEntities = Vector.empty[Entity]

      val coordinatorSettings = TestEntityCoordinatorSettings(shards, initialEntities,
        node(node1).address, myAddress)
      val coordinator = system.actorOf(TestEntityCoordinator.props(coordinatorSettings), "coordinator_2")
      val entityManager = system.actorOf(ShardManager.props(coordinator, TestEntity.propsCreator,
        TestEntity.entityReviver, myAddress, totalShards, self), "entities_2")

      enterBarrier("start-balancer-repair-missing")

      runOn(node1) {
        val nodes = Map(
          node(node1).address -> node(node1) / entityManager.path.elements,
          node(node2).address -> node(node2) / entityManager.path.elements,
          node(node3).address -> node(node3) / entityManager.path.elements,
          node(node4).address -> node(node4) / entityManager.path.elements,
          node(node5).address -> node(node5) / entityManager.path.elements
        )

        val monitor = TestProbe()
        system.actorOf(BalancerTask.props(coordinator, monitor.ref, nodes, totalShards), "balancer_2")
        val result = monitor.expectMsgClass(classOf[BalancerComplete])

        shards.get(0) shouldEqual AssignedShardEntry(0, node(node1).address)
        shards.get(1) shouldEqual AssignedShardEntry(1, node(node2).address)
        shards.get(2) shouldEqual AssignedShardEntry(2, node(node3).address)
        shards.get(3) shouldEqual AssignedShardEntry(3, node(node4).address)
        shards.get(4) shouldEqual AssignedShardEntry(4, node(node5).address)
      }

      enterBarrier("end-balancer-repair-missing")
    }

    "repair the shard map if a shard is frozen" in {

      val totalShards = 5
      val initialWidth = 1
      val shards = ShardMap(totalShards)
      shards.assign(0, node(node1).address)
      shards.assign(1, node(node2).address)
      shards.assign(2, node(node3).address)
      shards.assign(3, node(node3).address)
      shards.assign(4, node(node5).address)

      val initialEntities = Vector.empty[Entity]

      val coordinatorSettings = TestEntityCoordinatorSettings(shards, initialEntities,
        node(node1).address, myAddress)
      val coordinator = system.actorOf(TestEntityCoordinator.props(coordinatorSettings), "coordinator_3")
      val entityManager = system.actorOf(ShardManager.props(coordinator, TestEntity.propsCreator,
        TestEntity.entityReviver, myAddress, totalShards, self), "entities_3")

      enterBarrier("start-balancer-repair-frozen")

      runOn(node1) {
        val nodes = Map(
          node(node1).address -> node(node1) / entityManager.path.elements,
          node(node2).address -> node(node2) / entityManager.path.elements,
          node(node3).address -> node(node3) / entityManager.path.elements,
          node(node4).address -> node(node4) / entityManager.path.elements
        )

        val monitor = TestProbe()
        system.actorOf(BalancerTask.props(coordinator, monitor.ref, nodes, totalShards), "balancer_3")
        val result = monitor.expectMsgClass(classOf[BalancerComplete])

        shards.get(0) shouldEqual AssignedShardEntry(0, node(node1).address)
        shards.get(1) shouldEqual AssignedShardEntry(1, node(node2).address)
        shards.get(2) shouldEqual AssignedShardEntry(2, node(node3).address)
        shards.get(3) shouldEqual AssignedShardEntry(3, node(node3).address)
        shards.get(4) shouldEqual AssignedShardEntry(4, node(node4).address)
      }

      enterBarrier("end-balancer-repair-frozen")
    }
  }
}

