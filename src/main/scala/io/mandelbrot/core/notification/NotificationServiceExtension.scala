package io.mandelbrot.core.notification

import akka.actor._

/**
 *
 */
class NotificationServiceExtensionImpl(system: ActorSystem) extends Extension {
  val notificationService = system.actorOf(NotificationManager.props(), "notification-service")
}

/**
 *
 */
object NotificationServiceExtension extends ExtensionId[NotificationServiceExtensionImpl] with ExtensionIdProvider {
  override def lookup() = NotificationServiceExtension
  override def createExtension(system: ExtendedActorSystem) = new NotificationServiceExtensionImpl(system)
  override def get(system: ActorSystem): NotificationServiceExtensionImpl = super.get(system)
}

object NotificationService {
  def apply(system: ActorSystem): ActorRef = NotificationServiceExtension(system).notificationService
}
