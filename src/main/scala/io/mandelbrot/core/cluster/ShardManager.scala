package io.mandelbrot.core.cluster

import akka.actor._

/**
 *
 */
class ShardManager extends Actor with ActorLogging {

  def receive = {
    case _ =>
  }
}

object ShardManager {
  def props() = Props(classOf[ShardManager])
}

