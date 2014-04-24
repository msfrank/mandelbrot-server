package io.mandelbrot.core.history

import akka.actor._

/**
 *
 */
class HistoryServiceExtensionImpl(system: ActorSystem) extends Extension {
  val stateService = system.actorOf(HistoryManager.props(), "history-service")
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