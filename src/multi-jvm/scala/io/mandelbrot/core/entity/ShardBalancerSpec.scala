package io.mandelbrot.core.entity

import akka.testkit.{TestProbe, ImplicitSender}
import scala.concurrent.duration._

class ShardBalancerSpecMultiJvmNode1 extends ShardBalancerSpec
class ShardBalancerSpecMultiJvmNode2 extends ShardBalancerSpec
class ShardBalancerSpecMultiJvmNode3 extends ShardBalancerSpec
class ShardBalancerSpecMultiJvmNode4 extends ShardBalancerSpec
class ShardBalancerSpecMultiJvmNode5 extends ShardBalancerSpec

class ShardBalancerSpec extends MultiNodeSpec(RemoteMultiNodeConfig) with ImplicitSender {
  import ClusterMultiNodeConfig._

  def initialParticipants = roles.size

  "A ShardBalancer" should {

    "perform no operations if shard map is balanced" in {

      val totalShards = 5

      val shards = ShardMap(totalShards)
      shards.assign(0, node(node1).address)
      shards.assign(1, node(node2).address)
      shards.assign(2, node(node3).address)
      shards.assign(3, node(node4).address)
      shards.assign(4, node(node5).address)

      val initialEntities = Vector.empty[Entity]

      val coordinatorSettings = TestCoordinatorSettings(shards, initialEntities, node(node1).address, myAddress)
      val coordinator = system.actorOf(TestCoordinator.props(coordinatorSettings), "coordinator_1")
      val entityManager = system.actorOf(ShardManager.props(coordinator,
        TestEntity.shardResolver, TestEntity.keyExtractor, TestEntity.propsCreator, myAddress, totalShards),
        "entities_1")

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
        system.actorOf(ShardBalancer.props(coordinator, monitor.ref, nodes, totalShards), "balancer_1")
        val result = monitor.expectMsgClass(classOf[ShardBalancerResult])

        result.shardMap.get(0) shouldEqual AssignedShardEntry(0, node(node1).address)
        result.shardMap.get(1) shouldEqual AssignedShardEntry(1, node(node2).address)
        result.shardMap.get(2) shouldEqual AssignedShardEntry(2, node(node3).address)
        result.shardMap.get(3) shouldEqual AssignedShardEntry(3, node(node4).address)
        result.shardMap.get(4) shouldEqual AssignedShardEntry(4, node(node5).address)
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

      val coordinatorSettings = TestCoordinatorSettings(shards, initialEntities, node(node1).address, myAddress)
      val coordinator = system.actorOf(TestCoordinator.props(coordinatorSettings), "coordinator_2")
      val entityManager = system.actorOf(ShardManager.props(coordinator,
        TestEntity.shardResolver, TestEntity.keyExtractor, TestEntity.propsCreator, myAddress, totalShards),
        "entities_2")

      enterBarrier("start-balancer-repair-shard")

      runOn(node1) {
        val nodes = Map(
          node(node1).address -> node(node1) / entityManager.path.elements,
          node(node2).address -> node(node2) / entityManager.path.elements,
          node(node3).address -> node(node3) / entityManager.path.elements,
          node(node4).address -> node(node4) / entityManager.path.elements,
          node(node5).address -> node(node5) / entityManager.path.elements
        )

        val monitor = TestProbe()
        system.actorOf(ShardBalancer.props(coordinator, monitor.ref, nodes, totalShards), "balancer_2")
        val result = monitor.expectMsgClass(classOf[ShardBalancerResult])

        result.shardMap.get(0) shouldEqual AssignedShardEntry(0, node(node1).address)
        result.shardMap.get(1) shouldEqual AssignedShardEntry(1, node(node2).address)
        result.shardMap.get(2) shouldEqual AssignedShardEntry(2, node(node3).address)
        result.shardMap.get(3) shouldEqual AssignedShardEntry(3, node(node4).address)
        result.shardMap.get(4) shouldEqual AssignedShardEntry(4, node(node5).address)
      }

      enterBarrier("end-balancer-repair-shard")
    }
  }
}

