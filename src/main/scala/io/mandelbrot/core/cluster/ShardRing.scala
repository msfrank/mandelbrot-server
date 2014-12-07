package io.mandelbrot.core.cluster

import akka.actor.Address
import scala.collection.JavaConversions._

/**
 *
 */
class ShardRing {
  private val _shards = new java.util.TreeMap[Int,(Int,Address)]()

  def get(shardId: Int): Option[(Int,Address)] = Option(_shards.get(shardId))
  def put(shardId: Int, width: Int, address: Address): Unit = _shards.put(shardId,(width,address))
  def remove(shardId: Int): Unit = _shards.remove(shardId)

  def apply(key: Int): Option[(Int,Address)] = {
    var entry = _shards.floorEntry(key)
    if (entry == null)
      entry = _shards.lastEntry()
    if (entry != null) {
      val shardId = entry.getKey
      val (width,address) = entry.getValue
      val upperBound = shardId + width
      if (key >= shardId && key < upperBound) Some(shardId -> address) else None
    } else None
  }

  def contains(key: Int): Boolean = apply(key).isDefined

  def size = _shards.size()
  def isEmpty = _shards.isEmpty
  def nonEmpty = !isEmpty

  override def toString = {
    val shards = _shards.map { case (shardId,(width,address)) =>
        "%d:%d => %s".format(shardId, shardId + width, address.toString)
    }
    "ShardRing(%s)".format(shards)
  }
}

object ShardRing {
  def apply() = new ShardRing
}
