package io.mandelbrot.core.cluster

import akka.cluster.Cluster
import akka.actor._

import io.mandelbrot.core.ServerConfig
import io.mandelbrot.core.system.{ProbeSystemOperation, ProbeOperation, ProbeSystem}
import io.mandelbrot.core.registry.{RegistryServiceQuery, RegistryServiceCommand}

/**
 * 
 */
class ClusterCoordinator(registryService: ActorRef) extends Actor with ActorLogging {

  // config
  val settings = ServerConfig(context.system).settings.cluster

  // state
  var running = false

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

  val clusterMonitor = context.actorOf(ClusterMonitor.props(settings.minNrMembers), "cluster-monitor")
  val entityManager = context.actorOf(EntityManager.props(keyExtractor, propsCreator), "entity-manager")

  log.info("server is running in cluster mode")

  override def preStart(): Unit = {
    Cluster(context.system).joinSeedNodes(settings.seedNodes.map(AddressFromURIString(_)).toSeq)
  }

  override def postStop(): Unit = {
    Cluster(context.system).unsubscribe(self)
  }

  def receive = {

    case event: ClusterUp =>
      running = true
      entityManager ! event

    case event: ClusterDown =>
      running = false
      entityManager ! event

    case op: RegistryServiceQuery =>
      registryService forward op

    case op: RegistryServiceCommand =>
      entityManager forward op

    case op: ProbeSystemOperation =>
      entityManager forward op

    case op: ProbeOperation =>
      entityManager forward op
 
  }
}

object ClusterCoordinator {
  def props(registryService: ActorRef) = Props(classOf[ClusterCoordinator], registryService)
}
