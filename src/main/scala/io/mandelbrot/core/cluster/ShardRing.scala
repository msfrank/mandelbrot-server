package io.mandelbrot.core.cluster

import akka.actor.Address
import scala.collection.JavaConversions._

/**
 *
 */
class ShardRing {
  private val shards = new java.util.TreeMap[Int,(Int,Address)]()

  def get(shardId: Int): Option[(Int,Address)] = Option(shards.get(shardId))
  def put(shardId: Int, width: Int, address: Address): Unit = shards.put(shardId,(width,address))
  def remove(shardId: Int): Unit = shards.remove(shardId)

  def apply(key: Int): Option[(Int,Address)] = {
    var entry = shards.floorEntry(key)
    if (entry != null) {
      val shardId = entry.getKey
      val (width,address) = entry.getValue
      val upperBound = shardId + width
      if (key >= shardId && key < upperBound) Some(shardId -> address) else None
    } else None
  }

  def contains(key: Int): Boolean = apply(key).isDefined

  def size = shards.size()
  def isEmpty = shards.isEmpty
  def nonEmpty = !isEmpty

  def isFull: Boolean = {
    var toCheck = 0
    shards.foreach { case (shardId,(width, _)) =>
      if (shardId != toCheck)
        return false
      toCheck = toCheck + width
    }
    if (toCheck == Int.MinValue) true else false
  }
  def nonFull = !isFull

  override def toString = {
    val _shards = shards.map { case (shardId,(width,address)) =>
        "%d:%d => %s".format(shardId, shardId + width, address.toString)
    }
    "ShardRing(%s)".format(_shards)
  }
}

object ShardRing {
  def apply() = new ShardRing

  val ORDER1 = 33554432     // 64 shards in order 1
  val ORDER2 = 16777216     // 128 shards in order 2
  val ORDER3 = 8388608      // 256 shards in order 3
  val ORDER4 = 4194304      // 512 shards in order 4
  val ORDER5 = 2097152      // 1024 shards in order 5
}
