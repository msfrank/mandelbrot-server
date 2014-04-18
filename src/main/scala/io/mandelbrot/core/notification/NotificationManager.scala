package io.mandelbrot.core.notification

import akka.actor.{Props, ActorLogging, Actor}

class NotificationManager extends Actor with ActorLogging {

  def receive = {

    case notification: ProbeNotification =>
      log.debug("received notification {}", notification)

  }
}

object NotificationManager {
  def props() = Props(classOf[NotificationManager])
}
