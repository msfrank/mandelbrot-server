package io.mandelbrot.core.cluster

import akka.actor.Address

/**
 *
 */
class ShardMap(val totalShards: Int, val initialWidth: Int) {

  private class ShardEntry {
    var address: Option[Address] = None
    var left: Option[ShardEntry] = None
    var right: Option[ShardEntry] = None
  }

  private val shards = Array.fill[ShardEntry](totalShards / initialWidth)(new ShardEntry)

  /**
   * get the address for the specified shardId, or None if no entry exists.
   */
  def get(shardId: Int): Option[Address] = {
    if (shardId < 0 || shardId >= totalShards)
      throw new IllegalArgumentException()
    val entry = shards(shardId / initialWidth)
    entry.address
  }

  /**
   * set the address for the specified shard, returning the previous shard entry
   * if it exists, otherwise None.
   */
  def put(shardId: Int, address: Address): Option[Address] = {
    if (shardId < 0 || shardId >= totalShards)
      throw new IllegalArgumentException()
    val entry = shards(shardId / initialWidth)
    val prev = entry.address
    entry.address = Some(address)
    prev
  }

  /**
   * remove the specified shard from the ShardMap, returning the previous shard
   * entry if it exists, otherwise None.
   */
  def remove(shardId: Int): Option[Address] = {
    if (shardId < 0 || shardId >= totalShards)
      throw new IllegalArgumentException()
    val entry = shards(shardId / initialWidth)
    val prev = entry.address
    entry.address = None
    prev
  }

  /**
   * return the sequence of all shards which are assigned to an Address.
   */
  def assigned: Vector[(Int,Address)] = {
    0.until(shards.size).filter(shards(_).address.nonEmpty).map {
      i => i -> shards(i).address.get
    }.toVector
  }

  /**
   * return the set of all shardIds which have no associated Address.
   */
  def missing: Set[Int] = 0.until(shards.size).filter(shards(_).address.isEmpty).toSet

  /**
   *
   */
  def findShardId(shardKey: Int): Int = (scala.math.abs(shardKey) % totalShards) / initialWidth

  /**
   * given the specified shardKey, return the associated shardId and address if it
   * exists, otherwise None.
   */
  def apply(shardKey: Int): Option[(Int,Address)] = {
    val shardId = (scala.math.abs(shardKey) % totalShards) / initialWidth
    shards(shardId).address.map((shardId, _))
  }

  /**
   * returns true if the ShardMap contains the specified shardId, otherwise false.
   */
  def contains(shardKey: Int): Boolean = apply(shardKey).isDefined

  /**
   * returns the number of shards which have an associated address.
   */
  def size: Int = shards.foldLeft(0)((n,entry) => if (entry.address.isDefined) n + 1 else n)

  def isEmpty: Boolean = size == 0

  def nonEmpty: Boolean = !isEmpty

  def isFull: Boolean = {
    shards.foreach { case entry => if (entry.address.isEmpty) return false }
    true
  }

  def nonFull: Boolean = !isFull

  override def toString = {
    val active = for {
      i <- 0.until(shards.size)
      address <- shards(i).address
    } yield i + "=" + address
    "ShardMap(" + active.mkString(",") + ")"
  }
}

object ShardMap {
  def apply(totalShards: Int, initialWidth: Int) = new ShardMap(totalShards, initialWidth)
}
