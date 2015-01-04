package io.mandelbrot.core.cluster

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
      val initialWidth = 1
      val shards = ShardMap(totalShards, initialWidth)
      shards.assign(0, node(node1).address)
      shards.assign(1, node(node2).address)
      shards.assign(2, node(node3).address)
      shards.assign(3, node(node4).address)
      shards.assign(4, node(node5).address)

      val coordinator = system.actorOf(TestCoordinator.props(shards), "coordinator_1")
      val entityManager = system.actorOf(EntityManager.props(coordinator,
        TestEntity.shardResolver, TestEntity.keyExtractor, TestEntity.propsCreator, myAddress, totalShards, initialWidth),
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
        system.actorOf(ShardBalancer.props(coordinator, monitor.ref, nodes, totalShards, initialWidth), "balancer_1")
        monitor.expectMsgClass(classOf[ShardBalancerResult])
      }

      enterBarrier("end-balancer-no-ops")
    }

    "repair the shard map if a shard is missing" in {

      val totalShards = 5
      val initialWidth = 1
      val shards = ShardMap(totalShards, initialWidth)
      shards.assign(0, node(node1).address)
      //shards.assign(1, node(node2).address)
      shards.assign(2, node(node3).address)
      shards.assign(3, node(node4).address)
      shards.assign(4, node(node5).address)

      val coordinator = system.actorOf(TestCoordinator.props(shards), "coordinator_2")
      val entityManager = system.actorOf(EntityManager.props(coordinator,
        TestEntity.shardResolver, TestEntity.keyExtractor, TestEntity.propsCreator, myAddress, totalShards, initialWidth),
        "entities_2")

      enterBarrier("start-balancer-repair-shard")
      log.debug("passed barrier")

      runOn(node1) {
        val nodes = Map(
          node(node1).address -> node(node1) / entityManager.path.elements,
          node(node2).address -> node(node2) / entityManager.path.elements,
          node(node3).address -> node(node3) / entityManager.path.elements,
          node(node4).address -> node(node4) / entityManager.path.elements,
          node(node5).address -> node(node5) / entityManager.path.elements
        )

        val monitor = TestProbe()
        system.actorOf(ShardBalancer.props(coordinator, monitor.ref, nodes, totalShards, initialWidth), "balancer_2")
        monitor.expectMsgClass(classOf[ShardBalancerResult])
      }

      runOn(node2, node3, node4, node5) {
        expectNoMsg(10.seconds)
      }

      enterBarrier("end-balancer-repair-shard")
    }
  }
}

