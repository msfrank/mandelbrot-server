package io.mandelbrot.core.notification

import akka.actor.{Props, ActorLogging, Actor}
import io.mandelbrot.core.ServerConfig

class NotificationManager extends Actor with ActorLogging {

  // config
  val settings = ServerConfig(context.system).settings.notifications

  def receive = {

    case notification: Notification =>
      log.debug("received notification {}", notification)
      // TODO: store the notification in history

  }
}

object NotificationManager {
  def props() = Props(classOf[NotificationManager])
}
