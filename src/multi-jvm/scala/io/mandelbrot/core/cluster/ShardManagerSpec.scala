package io.mandelbrot.core.cluster

import akka.testkit.ImplicitSender
import akka.cluster.Cluster
import scala.concurrent.duration._

class ShardManagerSpecMultiJvmNode1 extends ShardManagerSpec
class ShardManagerSpecMultiJvmNode2 extends ShardManagerSpec
class ShardManagerSpecMultiJvmNode3 extends ShardManagerSpec
class ShardManagerSpecMultiJvmNode4 extends ShardManagerSpec
class ShardManagerSpecMultiJvmNode5 extends ShardManagerSpec

class ShardManagerSpec extends ClusterMultiNodeSpec(ClusterMultiNodeConfig) with ImplicitSender {
  import ClusterMultiNodeConfig._

  def initialParticipants = roles.size

  "A ShardManager cluster" must {

    "wait for all nodes to become ready" in {
      system.eventStream.subscribe(testActor, classOf[ShardManagerEvent])
      enterBarrier("startup")
      system.actorOf(ShardManager.props(), "shard-manager")
      Cluster(system).join(node(node1).address)
      expectMsg(30.seconds, ShardClusterUp)
    }

  }
}
