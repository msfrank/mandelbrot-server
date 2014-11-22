package io.mandelbrot.core.cluster

import akka.testkit.{TestProbe, ImplicitSender}
import akka.cluster.Cluster
import scala.concurrent.duration._

import io.mandelbrot.core.ChildForwarder

class ShardManagerSpecMultiJvmNode1 extends ShardManagerSpec
class ShardManagerSpecMultiJvmNode2 extends ShardManagerSpec
class ShardManagerSpecMultiJvmNode3 extends ShardManagerSpec
class ShardManagerSpecMultiJvmNode4 extends ShardManagerSpec
class ShardManagerSpecMultiJvmNode5 extends ShardManagerSpec

class ShardManagerSpec extends ClusterMultiNodeSpec(ClusterMultiNodeConfig) with ImplicitSender {
  import ClusterMultiNodeConfig._

  def initialParticipants = roles.size

  "A ShardManager cluster" should {

    "wait for nodes to become ready and initial balancing" in {

      system.eventStream.subscribe(testActor, classOf[ShardManagerEvent])
      val props = ShardManager.props(initialParticipants, 64)
      val probe = TestProbe()
      val childForwarder = system.actorOf(ChildForwarder.props(props, probe.ref), "forwarder")

      Cluster(system).join(node(node1).address)

      expectMsg(30.seconds, ShardClusterUp)
      val proposal = probe.expectMsgClass(30.seconds, classOf[RebalanceProposal])
      probe.reply(AppliedProposal(proposal))
      expectMsg(30.seconds, ShardClusterRebalances)

      enterBarrier("cluster-up")
    }
  }
}
