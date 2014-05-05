package io.mandelbrot.core.registry

import akka.actor._
import io.mandelbrot.core.{ServiceExtension, ServerConfig}
import org.slf4j.LoggerFactory

/**
 *
 */
class RegistryServiceExtensionImpl(system: ActorSystem) extends ServiceExtension {
  val registryService = {
    val settings = ServerConfig(system).settings.registry
    val plugin = settings.plugin
    val service = settings.service
    LoggerFactory.getLogger("io.mandelbrot.core.registry.RegistryServiceExtension").info("loading plugin " + plugin)
    system.actorOf(makeServiceProps(plugin, service), "registry-service")
  }
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