package io.mandelbrot.core.cluster

import akka.actor.RootActorPath
import akka.cluster.Cluster
import akka.testkit.{TestProbe, ImplicitSender}
import scala.concurrent.duration._

class ShardBalancerSpecMultiJvmNode1 extends ShardBalancerSpec
class ShardBalancerSpecMultiJvmNode2 extends ShardBalancerSpec
class ShardBalancerSpecMultiJvmNode3 extends ShardBalancerSpec
class ShardBalancerSpecMultiJvmNode4 extends ShardBalancerSpec
class ShardBalancerSpecMultiJvmNode5 extends ShardBalancerSpec

class ShardBalancerSpec extends ClusterMultiNodeSpec(ClusterMultiNodeConfig) with ImplicitSender {
  import ClusterMultiNodeConfig._

  def initialParticipants = roles.size

  "A ShardBalancer" should {

    "perform no operations if shard map is balanced" in {

      val totalShards = 5
      val initialWidth = 1
      val shards = ShardMap(totalShards, initialWidth)
      shards.put(0, node(node1).address)
      shards.put(1, node(node2).address)
      shards.put(2, node(node3).address)
      shards.put(3, node(node4).address)
      shards.put(4, node(node5).address)

      val coordinator = system.actorOf(TestCoordinator.props(shards), "coordinator_1")
      val entityManager = system.actorOf(EntityManager.props(coordinator,
        TestEntity.shardResolver, TestEntity.keyExtractor, TestEntity.propsCreator, totalShards, initialWidth),
        "entities_1")

      val eventStream = TestProbe()
      system.eventStream.subscribe(eventStream.ref, classOf[ClusterUp])
      val clusterMonitor = system.actorOf(ClusterMonitor.props(initialParticipants), "cluster-monitor")
      Cluster(system).join(node(node1).address)
      eventStream.expectMsgClass(30.seconds, classOf[ClusterUp])
      enterBarrier("cluster-up")

      runOn(node1) {
        val nodes = Map(
          node(node1).address -> RootActorPath(node(node1).address, "/entities_1"),
          node(node2).address -> RootActorPath(node(node2).address, "/entities_1"),
          node(node3).address -> RootActorPath(node(node3).address, "/entities_1"),
          node(node4).address -> RootActorPath(node(node4).address, "/entities_1"),
          node(node5).address -> RootActorPath(node(node5).address, "/entities_1")
        )
        val monitor = TestProbe()
        val shardBalancer = system.actorOf(ShardBalancer.props(coordinator, monitor.ref, nodes, totalShards, initialWidth))
        val result = monitor.expectMsgClass(classOf[ShardBalancerResult])
      }

      enterBarrier("balancer-no-ops")
    }
  }
}

