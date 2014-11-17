package io.mandelbrot.core.cluster

import akka.actor.Address
import scala.collection.JavaConversions._

/**
 *
 */
class ShardRing extends Serializable {
  private val _shards = new java.util.TreeMap[Int,Address]()

  def get(shard: Int): Option[Address] = Option(_shards.get(shard))
  def put(shard: Int, address: Address): Unit = _shards.put(shard, address)
  def remove(shard: Int): Unit = _shards.remove(shard)

  def shards: Iterable[(Int,Address)] = _shards.entrySet().toIterable.map(entry => entry.getKey -> entry.getValue)

  def apply(key: String): Option[Address] = get(key.hashCode)

  def size = _shards.size()
  def isEmpty = _shards.isEmpty
  def nonEmpty = !isEmpty

  //def split()
  //def merge()

  override def toString = "ShardRing(%s)".format(shards.map(s => "%s:%s".format(s._1, s._2)).mkString(","))
}

object ShardRing {

  def apply(cluster: Set[Address], initialShards: Int = 64): ShardRing = {
    val ring = new ShardRing
    val addresses = cluster.toVector
    val base = Int.MaxValue / initialShards
    0.until(initialShards).foreach { n =>
      val shard = n * base
      val address = addresses(n % cluster.size)
      ring.put(shard, address)
    }
    ring
  }

  def apply() = new ShardRing
}