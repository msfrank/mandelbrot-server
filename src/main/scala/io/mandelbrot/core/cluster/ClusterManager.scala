package io.mandelbrot.core.cluster

import akka.cluster.Cluster
import akka.actor._

import io.mandelbrot.core.{BadRequest, ApiException, ServiceExtension}
import io.mandelbrot.core.cluster.EntityFunctions.{ShardResolver, KeyExtractor, PropsCreator}

/**
 * 
 */
class ClusterManager(settings: ClusterSettings,
                     shardResolver: ShardResolver,
                     keyExtractor: KeyExtractor,
                     propsCreator: PropsCreator) extends Actor with ActorLogging {

  // state
  var incubating = true
  var running = false
  var status: ClusterMonitorEvent = ClusterUnknown

  val coordinator = {
    val props = ServiceExtension.makePluginProps(settings.coordinator.plugin, settings.coordinator.settings)
    log.info("loading coordinator plugin {}", settings.coordinator.plugin)
    context.actorOf(props, "coordinator")
  }
  val entityManager = context.actorOf(EntityManager.props(coordinator, shardResolver, keyExtractor, propsCreator), "entity-manager")
  val clusterMonitor = context.actorOf(ClusterMonitor.props(settings.minNrMembers), "cluster-monitor")

  log.info("initializing cluster mode")

  override def preStart(): Unit = {
    if (settings.seedNodes.nonEmpty) {
      Cluster(context.system).joinSeedNodes(settings.seedNodes.map(AddressFromURIString(_)).toSeq)
      self ! JoinCluster(settings.seedNodes.toVector)
      log.info("joining cluster using seed nodes {}", settings.seedNodes.mkString(","))
    } else
      log.info("waiting for seed nodes")
  }

  def receive = {

    // try to join the cluster using the specified seed nodes
    case op: JoinCluster =>
      if (incubating) {
        val seedNodes = op.seedNodes.map(AddressFromURIString(_)).toSeq
        Cluster(context.system).joinSeedNodes(seedNodes)
        incubating = false
        log.debug("attempting to join cluster")
      } else
        sender() ! ClusterServiceOperationFailed(op, new ApiException(BadRequest))

    // cluster monitor emits this message
    case event: ClusterUp =>
      running = true
      status = event

    // cluster monitor emits this message
    case event: ClusterDown =>
      running = false
      status = event

    // return the current cluster status known by the cluster monitor
    case op: GetClusterStatus =>
      sender() ! GetClusterStatusResult(op, status)

    // send envelopes directly to the entity manager
    case envelope: EntityEnvelope =>
      entityManager ! envelope

    // we assume any other message is for an entity, so we wrap it in an envelope
    case message: Any =>
      entityManager ! EntityEnvelope(sender(), message, attempts = 3)
  }
}

object ClusterManager {
  def props(settings: ClusterSettings, shardResolver: ShardResolver, keyExtractor: KeyExtractor, propsCreator: PropsCreator) = {
    Props(classOf[ClusterManager], settings, shardResolver, keyExtractor, propsCreator)
  }
}

sealed trait ClusterServiceOperation
sealed trait ClusterServiceCommand extends ClusterServiceOperation
sealed trait ClusterServiceQuery extends ClusterServiceOperation
case class ClusterServiceOperationFailed(op: ClusterServiceOperation, failure: Throwable)

case class JoinCluster(seedNodes: Vector[String]) extends ClusterServiceCommand
case class JoinClusterResult(op: JoinCluster)

case class UpdateMemberShards(allocations: Map[Int,Int]) extends ClusterServiceCommand
case class UpdateMemberShardsResult(op: UpdateMemberShards)

case class GetClusterStatus() extends ClusterServiceQuery
case class GetClusterStatusResult(op: GetClusterStatus, status: ClusterMonitorEvent)

case class GetShard(shardKey: Int) extends ClusterServiceQuery
case class GetShardResult(op: GetShard, shardId: Int, width: Int, address: Address)

/* marker trait for Coordinator implementations */
trait Coordinator
