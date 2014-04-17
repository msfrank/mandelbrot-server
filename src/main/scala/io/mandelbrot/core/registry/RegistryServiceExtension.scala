package io.mandelbrot.core.registry

import akka.actor._

/**
 *
 */
class RegistryServiceExtensionImpl(system: ActorSystem) extends Extension {
  val registryService = system.actorOf(RegistryManager.props(), "registry-service")
}

/**
 *
 */
object RegistryServiceExtension extends ExtensionId[RegistryServiceExtensionImpl] with ExtensionIdProvider {
  override def lookup() = RegistryServiceExtension
  override def createExtension(system: ExtendedActorSystem) = new RegistryServiceExtensionImpl(system)
  override def get(system: ActorSystem): RegistryServiceExtensionImpl = super.get(system)
}

object RegistryService {
  def apply(system: ActorSystem): ActorRef = RegistryServiceExtension(system).registryService
}