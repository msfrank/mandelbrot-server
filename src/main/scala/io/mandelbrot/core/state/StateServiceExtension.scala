package io.mandelbrot.core.state

import akka.actor._

/**
 *
 */
class StateServiceExtensionImpl(system: ActorSystem) extends Extension {
  val stateService = system.actorOf(StateManager.props(), "state-service")
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