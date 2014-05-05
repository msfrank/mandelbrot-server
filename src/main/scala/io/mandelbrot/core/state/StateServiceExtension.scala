package io.mandelbrot.core.state

import akka.actor._
import io.mandelbrot.core.{ServerConfig, ServiceExtension}
import org.slf4j.LoggerFactory

/**
 *
 */
class StateServiceExtensionImpl(system: ActorSystem) extends ServiceExtension {
  val stateService = {
    val settings = ServerConfig(system).settings.state
    val plugin = settings.plugin
    val service = settings.service
    LoggerFactory.getLogger("io.mandelbrot.core.state.StateServiceExtension").info("loading plugin " + plugin)
    system.actorOf(makeServiceProps(plugin, service), "state-service")
  }
}

/**
 *
 */
object StateServiceExtension extends ExtensionId[StateServiceExtensionImpl] with ExtensionIdProvider {
  override def lookup() = StateServiceExtension
  override def createExtension(system: ExtendedActorSystem) = new StateServiceExtensionImpl(system)
  override def get(system: ActorSystem): StateServiceExtensionImpl = super.get(system)
}

object StateService {
  def apply(system: ActorSystem): ActorRef = StateServiceExtension(system).stateService
}