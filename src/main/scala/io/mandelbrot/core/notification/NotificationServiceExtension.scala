package io.mandelbrot.core.notification

import akka.actor._

import io.mandelbrot.core.{ServerConfig, ServiceExtension}

/**
 *
 */
class NotificationServiceExtensionImpl(system: ActorSystem) extends ServiceExtension {
  val notificationService = {
    val settings = ServerConfig(system).settings.notifications
    val plugin = settings.plugin
    val service = settings.service
    system.actorOf(makeServiceProps(plugin, service), "notification-service")
  }
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
