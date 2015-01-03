package io.mandelbrot.core.cluster

import akka.actor.RootActorPath
import akka.cluster.Cluster
import akka.testkit.{TestProbe, ImplicitSender}
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.duration._

class ShardBalancerSpecMultiJvmNode1 extends ShardBalancerSpec
class ShardBalancerSpecMultiJvmNode2 extends ShardBalancerSpec
class ShardBalancerSpecMultiJvmNode3 extends ShardBalancerSpec
class ShardBalancerSpecMultiJvmNode4 extends ShardBalancerSpec
class ShardBalancerSpecMultiJvmNode5 extends ShardBalancerSpec

class ShardBalancerSpec extends ClusterMultiNodeSpec(ClusterMultiNodeConfig) with ImplicitSender with BeforeAndAfterAll {
  import ClusterMultiNodeConfig._

  def initialParticipants = roles.size

  override def beforeAll(): Unit = {
    val eventStream = TestProbe()
    system.eventStream.subscribe(eventStream.ref, classOf[ClusterUp])
    val clusterMonitor = system.actorOf(ClusterMonitor.props(initialParticipants), "cluster-monitor")
    Cluster(system).join(node(node1).address)
    eventStream.expectMsgClass(30.seconds, classOf[ClusterUp])
  }

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

      enterBarrier("start-balancer-no-ops")

      runOn(node1) {
        val nodes = Map(
          node(node1).address -> RootActorPath(node(node1).address, "/entities_1"),
          node(node2).address -> RootActorPath(node(node2).address, "/entities_1"),
          node(node3).address -> RootActorPath(node(node3).address, "/entities_1"),
          node(node4).address -> RootActorPath(node(node4).address, "/entities_1"),
          node(node5).address -> RootActorPath(node(node5).address, "/entities_1")
        )
        val monitor = TestProbe()
        system.actorOf(ShardBalancer.props(coordinator, monitor.ref, nodes, totalShards, initialWidth), "balancer_1")
        val result = monitor.expectMsgClass(classOf[ShardBalancerResult])
      }

      enterBarrier("end-balancer-no-ops")
    }

    "repair the shard map if a shard is missing" in {

      val totalShards = 5
      val initialWidth = 1
      val shards = ShardMap(totalShards, initialWidth)
      shards.put(0, node(node1).address)
      //shards.put(1, node(node2).address)
      shards.put(2, node(node3).address)
      shards.put(3, node(node4).address)
      shards.put(4, node(node5).address)

      val coordinator = system.actorOf(TestCoordinator.props(shards), "coordinator_2")
      val entityManager = system.actorOf(EntityManager.props(coordinator,
        TestEntity.shardResolver, TestEntity.keyExtractor, TestEntity.propsCreator, totalShards, initialWidth),
        "entities_2")

      enterBarrier("start-balancer-repair-shard")

      runOn(node1) {
        val nodes = Map(
          node(node1).address -> RootActorPath(node(node1).address, "/entities_2"),
          node(node2).address -> RootActorPath(node(node2).address, "/entities_2"),
          node(node3).address -> RootActorPath(node(node3).address, "/entities_2"),
          node(node4).address -> RootActorPath(node(node4).address, "/entities_2"),
          node(node5).address -> RootActorPath(node(node5).address, "/entities_2")
        )
        val monitor = TestProbe()
        system.actorOf(ShardBalancer.props(coordinator, monitor.ref, nodes, totalShards, initialWidth), "balancer_2")
        val result = monitor.expectMsgClass(classOf[ShardBalancerResult])
      }

      enterBarrier("end-balancer-repair-shard")
    }
  }
}

