package io.mandelbrot.core.cluster

import akka.actor._
import akka.cluster.Cluster
import scala.util.hashing.MurmurHash3

import io.mandelbrot.core.ServerConfig
import io.mandelbrot.core.system.{ProbeSystemOperation, ProbeOperation, ProbeSystem}
import io.mandelbrot.core.registry.{RegistryServiceQuery, RegistryServiceCommand}

/**
 * 
 */
class ClusterCoordinator(registryService: ActorRef) extends Actor with ActorLogging {

  val settings = ServerConfig(context.system).settings.cluster

  val shardResolver: EntityFunctions.ShardResolver = {
    case op: RegistryServiceCommand => 0
    case op: ProbeOperation => MurmurHash3.stringHash(op.probeRef.uri.toString)
    case op: ProbeSystemOperation => MurmurHash3.stringHash(op.uri.toString)
  }
  val keyExtractor: EntityFunctions.KeyExtractor = {
    case op: RegistryServiceCommand => "registry/"
    case op: ProbeOperation => "system/" + op.probeRef.uri.toString
    case op: ProbeSystemOperation => "system/" + op.uri.toString
  }
  val propsCreator: EntityFunctions.PropsCreator = {
    case op: RegistryServiceCommand => RegistryCoordinator.props(registryService)
    case op: ProbeOperation => ProbeSystem.props(context.parent)
    case op: ProbeSystemOperation => ProbeSystem.props(context.parent)
  }

  val shardManager = context.actorOf(ShardManager.props(settings.minNrMembers,
    settings.initialShardCount), "shard-manager")
  val entityManager = context.actorOf(EntityManager.props(shardResolver, keyExtractor,
    propsCreator), "entity-manager")

  log.info("server is running in cluster mode")

  override def preStart(): Unit = {
    Cluster(context.system).joinSeedNodes(settings.seedNodes.map(AddressFromURIString(_)).toSeq)
  }

  def receive = {

    case op: RegistryServiceCommand =>
      entityManager forward op

    case op: RegistryServiceQuery =>
      registryService forward op

    case op: ProbeSystemOperation =>
      entityManager forward op

    case op: ProbeOperation =>
      entityManager forward op
 
  }
}

object ClusterCoordinator {
  def props(registryService: ActorRef) = Props(classOf[ClusterCoordinator], registryService)
}