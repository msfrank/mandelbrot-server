package io.mandelbrot.core.entity

import akka.actor._
import scala.concurrent.duration.FiniteDuration

class MoveShardTask extends Actor with ActorLogging {

  def receive = {
    case _ =>
  }
}

object MoveShardTask {
  def props(op: MoveShard, coordinator: ActorRef, monitor: ActorRef, timeout: FiniteDuration) = {
    Props(classOf[MoveShardTask], op, coordinator, monitor, timeout)
  }
}

case class MoveShard(shardId: Int, targetNode: ActorPath)
case class MoveShardComplete(op: MoveShard)
case class MoveShardFailed(op: MoveShard, ex: Throwable)
