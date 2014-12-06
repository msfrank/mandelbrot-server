package io.mandelbrot.core.cluster

import akka.remote.testkit.MultiNodeConfig
import akka.testkit.{TestProbe, ImplicitSender}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

class ClusterManagerSpecMultiJvmNode1 extends ClusterManagerSpec
class ClusterManagerSpecMultiJvmNode2 extends ClusterManagerSpec
class ClusterManagerSpecMultiJvmNode3 extends ClusterManagerSpec
class ClusterManagerSpecMultiJvmNode4 extends ClusterManagerSpec
class ClusterManagerSpecMultiJvmNode5 extends ClusterManagerSpec

object ClusterManagerMultiNodeConfig extends MultiNodeConfig {
  commonConfig(ConfigFactory.parseString(
    """
      |include "multi-jvm.conf"
      |mandelbrot {
      |  cluster {
      |    enabled = true
      |    seed-nodes = []
      |    min-nr-members = 5
      |    shard-count = 100
      |    plugin = "io.mandelbrot.core.cluster.TestCoordinator"
      |    plugin-settings { }
      |  }
      |}
    """.stripMargin))
  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")
  val node4 = role("node4")
  val node5 = role("node5")
}

class ClusterManagerSpec extends ClusterMultiNodeSpec(ClusterManagerMultiNodeConfig) with ImplicitSender {
  import ClusterManagerMultiNodeConfig._

  def initialParticipants = roles.size

  val clusterSettings = new ClusterSettings(enabled = true,
                                            seedNodes = Vector.empty,
                                            minNrMembers = 5,
                                            shardCount = 100,
                                            CoordinatorSettings("io.mandelbrot.core.cluster.TestCoordinator", None))
  val eventStream = TestProbe()

  "A ClusterManager" should {

    "wait for nodes to become ready" in {

      val keyExtractor: EntityFunctions.KeyExtractor = {
        case m: TestCoordinatorMessage => m.key
      }
      val propsCreator: EntityFunctions.PropsCreator = {
        case m: TestCoordinatorMessage => TestEntity.props()
      }

      system.eventStream.subscribe(eventStream.ref, classOf[ClusterUp])
      val props = ClusterManager.props(clusterSettings, keyExtractor, propsCreator)
      val clusterManager = system.actorOf(props, "cluster-manager")
      clusterManager ! JoinCluster(Vector(node(node1).address.toString))

      eventStream.expectMsgClass(30.seconds, classOf[ClusterUp])

      enterBarrier("cluster-up")
    }
  }
}
