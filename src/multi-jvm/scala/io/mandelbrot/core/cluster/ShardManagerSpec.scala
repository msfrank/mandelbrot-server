//package io.mandelbrot.core.cluster
//
//import akka.testkit.{TestProbe, ImplicitSender}
//import akka.cluster.Cluster
//import scala.concurrent.duration._
//
//import io.mandelbrot.core.ChildForwarder
//
//class ShardManagerSpecMultiJvmNode1 extends ShardManagerSpec
//class ShardManagerSpecMultiJvmNode2 extends ShardManagerSpec
//class ShardManagerSpecMultiJvmNode3 extends ShardManagerSpec
//class ShardManagerSpecMultiJvmNode4 extends ShardManagerSpec
//class ShardManagerSpecMultiJvmNode5 extends ShardManagerSpec
//
//class ShardManagerSpec extends ClusterMultiNodeSpec(ClusterMultiNodeConfig) with ImplicitSender {
//  import ClusterMultiNodeConfig._
//
//  def initialParticipants = roles.size
//
//  "A ShardManager cluster" should {
//
//    "wait for nodes to become ready and initial balancing" in {
//
//      val props = ShardBalancer.props()
//      val managerProbe = TestProbe()
//      val shardManager = system.actorOf(ChildForwarder.props(props, managerProbe.ref), "fwd-shard-manager")
//      val monitorProbe = TestProbe()
//      val monitor = system.actorOf(ChildForwarder.props(ClusterMonitor.props(minNrMembers = 5), monitorProbe.ref), "fwd-cluster-monitor")
//
//      Cluster(system).join(node(node1).address)
//
//      monitorProbe.ignoreMsg { case event: ClusterDown => true }
//      val clusterEvent = monitorProbe.expectMsgClass(30.seconds, classOf[ClusterUp])
//
//      shardManager ! clusterEvent
//
//      val proposal = managerProbe.expectMsgClass(30.seconds, classOf[ApplyProposal])
//      managerProbe.reply(ApplyProposalResult(proposal))
//
//      managerProbe.expectMsg(30.seconds, ShardsRebalanced)
//
//      enterBarrier("cluster-up")
//    }
//  }
//}
