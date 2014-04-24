package io.mandelbrot.core.notification

import akka.actor.{Props, ActorLogging, Actor}
import io.mandelbrot.core.ServerConfig
import io.mandelbrot.core.history.HistoryService

class NotificationManager extends Actor with ActorLogging {

  // config
  val settings = ServerConfig(context.system).settings.notifications

  val historyService = HistoryService(context.system)

  def receive = {

    case notification: Notification =>
      historyService ! notification

  }
}

object NotificationManager {
  def props() = Props(classOf[NotificationManager])
}
