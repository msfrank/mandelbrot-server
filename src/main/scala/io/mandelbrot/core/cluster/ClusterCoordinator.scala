package io.mandelbrot.core.cluster

import akka.actor._
import akka.cluster.Cluster
import akka.contrib.pattern.{ClusterSingletonManager, ClusterSharding}

import io.mandelbrot.core.ServerConfig
import io.mandelbrot.core.system.{ProbeSystemOperation, ProbeOperation, ProbeSystem}
import io.mandelbrot.core.registry.{RegistryServiceQuery, RegistryServiceCommand}

/**
 * 
 */
class ClusterCoordinator(registryService: ActorRef) extends Actor with ActorLogging {

  val settings = ServerConfig(context.system).settings.cluster

  val registryCoordinator = context.actorOf(ClusterSingletonManager.props(
    singletonProps = RegistryCoordinator.props(registryService), singletonName = "command-coordinator",
    terminationMessage = PoisonPill, role = None), name = "command-coordinator")
  val systemEntries = ClusterSharding(context.system).start(typeName = "ProbeSystem",
    entryProps = Some(ProbeSystem.props(self)), idExtractor = ProbeSystem.idExtractor,
    shardResolver = ProbeSystem.shardResolver)

  log.info("server is running in cluster mode")

  override def preStart(): Unit = {
    Cluster(context.system).joinSeedNodes(settings.seedNodes.map(AddressFromURIString(_)).toSeq)
  }

  def receive = {
   
    case op: RegistryServiceQuery =>
      registryService forward op

    case op: RegistryServiceCommand =>
      registryCoordinator forward op

    case op: ProbeSystemOperation =>
      systemEntries forward op

    case op: ProbeOperation =>
      systemEntries forward op
 
  }
}

object ClusterCoordinator {
  def props(registryService: ActorRef) = Props(classOf[ClusterCoordinator], registryService)
}