package io.mandelbrot.core.history

import akka.actor._
import io.mandelbrot.core.{ServiceExtension, ServerConfig}
import org.slf4j.LoggerFactory

/**
 *
 */
class HistoryServiceExtensionImpl(system: ActorSystem) extends ServiceExtension {
  val stateService = {
    val settings = ServerConfig(system).settings.history
    val plugin = settings.plugin
    val service = settings.service
    LoggerFactory.getLogger("io.mandelbrot.core.history.HistoryServiceExtension").info("loading plugin " + plugin)
    system.actorOf(makeServiceProps(plugin, service), "history-service")
  }
}

/**
 *
 */
object HistoryServiceExtension extends ExtensionId[HistoryServiceExtensionImpl] with ExtensionIdProvider {
  override def lookup() = HistoryServiceExtension
  override def createExtension(system: ExtendedActorSystem) = new HistoryServiceExtensionImpl(system)
  override def get(system: ActorSystem): HistoryServiceExtensionImpl = super.get(system)
}

object HistoryService {
  def apply(system: ActorSystem): ActorRef = HistoryServiceExtension(system).stateService
}