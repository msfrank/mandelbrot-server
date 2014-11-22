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

  def apply(key: Int): Option[Address] = {
    var entry = _shards.floorEntry(key)
    if (entry == null) {
      entry = _shards.lastEntry()
    }
    if (entry == null) None else Some(entry.getValue)
  }

  def size = _shards.size()
  def isEmpty = _shards.isEmpty
  def nonEmpty = !isEmpty

  def +=(mutation: ShardRingMutation): ShardRing = {
    mutation.mutate(this)
    this
  }

  def +=(mutations: Iterable[ShardRingMutation]): ShardRing = {
    mutations.foreach(_.mutate(this))
    this
  }

  override def toString = "ShardRing(%s)".format(shards.map(s => "%s:%s".format(s._1, s._2)).mkString(", "))
}

object ShardRing {


  def apply() = new ShardRing
}

sealed trait ShardRingMutation {
  def mutate(shardRing: ShardRing): Unit
}

/**
 *
 */
case class MoveShard(shard: Int, address: Address) extends ShardRingMutation {
  def mutate(shardRing: ShardRing): Unit = shardRing.put(shard, address)
}

/**
 *
 */
case class InitializeRing(initial: Map[Address,Set[Int]]) extends ShardRingMutation {
  def mutate(shardRing: ShardRing): Unit = {
    initial.foreach { case (address, shards) =>
      shards.foreach(shard => shardRing.put(shard, address))
    }
  }
}

object InitializeRing {
  def apply(addresses: Vector[Address], initialShards: Int): InitializeRing = {
    val base = Int.MaxValue / initialShards
    val proposed = 0.until(initialShards).foldLeft(Map.empty[Address,Set[Int]]) { case (m, i) =>
      val shard = i * base
      val address = addresses(i % addresses.size)
      m + (address -> (m.getOrElse(address, Set.empty[Int]) + shard))
    }
    InitializeRing(proposed)
  }
}

//case class SplitShard() extends ShardRingMutation
//case class MergeShard() extends ShardRingMutation
