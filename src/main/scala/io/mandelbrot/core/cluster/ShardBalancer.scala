package io.mandelbrot.core.cluster

import akka.actor._

/**
 *
 */
class ShardBalancer(coordinator: ActorRef, settings: ClusterSettings) extends Actor with ActorLogging {

  val shardMap = ShardMap(settings.totalShards, settings.initialWidth)

  override def preStart(): Unit = {
    coordinator ! GetAllShards()
  }

  def receive = {
    case result: GetAllShardsResult =>
//      result.shards.foreach { case (shardId, width, address) => shards.put(shardId, width, address) }
//      if (result.finished) {
//        // check if ring is missing any shards
//        if (shards.nonFull) {
//
//        }
//      }
  }
}

object ShardBalancer {
  def props() = Props(classOf[ShardBalancer])
}
