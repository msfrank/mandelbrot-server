package io.mandelbrot.core.cluster

import akka.actor.Address

/**
 *
 */
class ShardMap(val totalShards: Int, val initialWidth: Int) {

  // FIXME: assert invariants

  private class MapEntry(_shard: ShardEntry) {
    var shard: ShardEntry = _shard
    var left: Option[MapEntry] = None
    var right: Option[MapEntry] = None
  }

  // initialize the shard map with missing entries
  private val entries = new Array[MapEntry](totalShards / initialWidth)
  0.until(entries.length).foreach { n =>
    val shardId = initialWidth * n
    val width = initialWidth
    entries(n) = new MapEntry(MissingShardEntry(shardId, width))
  }

  private var numMissing = entries.length

  /**
   * get the shard entry for the specified shardId.
   */
  private def getShardEntry(shardId: Int): MapEntry = {
    if (shardId < 0 || shardId >= totalShards)
      throw new IllegalArgumentException("shardId %d is out of range".format(shardId))
    entries(shardId / initialWidth)
  }

  /**
   * get the shard for the specified shardId.
   */
  def get(shardId: Int): ShardEntry = getShardEntry(shardId).shard

  /**
   * convert the specified shardKey into a shardId.
   */
  def getShardId(shardKey: Int): Int = (scala.math.abs(shardKey) % totalShards) / initialWidth

  /**
   * given the specified shardKey, return the associated shard.
   */
  def apply(shardKey: Int): ShardEntry = entries(getShardId(shardKey)).shard

  /**
   * assign an address to the specified shard, returning the previous shard.
   */
  def assign(shardId: Int, address: Address): ShardEntry = {
    val entry = getShardEntry(shardId)
    val prev = entry.shard
    entry.shard = AssignedShardEntry(prev.shardId, prev.width, address)
    if (prev.isEmpty)
      numMissing = numMissing - 1
    prev
  }

  /**
   * assign an address to the specified shard, returning the previous shard.
   */
  def prepare(shardId: Int, address: Address): ShardEntry = {
    val entry = getShardEntry(shardId)
    val prev = entry.shard
    entry.shard = PreparingShardEntry(prev.shardId, prev.width, address)
    if (prev.isEmpty)
      numMissing = numMissing - 1
    prev
  }

  /**
   * assign an address to the specified shard, returning the previous shard.
   */
  def migrate(shardId: Int, address: Address): ShardEntry = {
    val entry = getShardEntry(shardId)
    val prev = entry.shard
    entry.shard = MigratingShardEntry(prev.shardId, prev.width, address)
    if (prev.isEmpty)
      numMissing = numMissing - 1
    prev
  }

  /**
   * remove the specified shard from the ShardMap, returning the previous shard.
   */
  def remove(shardId: Int): ShardEntry = {
    val entry = getShardEntry(shardId)
    val prev = entry.shard
    entry.shard = MissingShardEntry(prev.shardId, prev.width)
    if (prev.isDefined)
      numMissing = numMissing + 1
    prev
  }

  /**
   * return the sequence containing all shard entries.
   */
  def shards: Vector[ShardEntry] = entries.map(_.shard).toVector

  /**
   * return the sequence of all shards which are assigned to an Address.
   */
  def assigned: Vector[AssignedShardEntry] = {
    entries.filter(_.shard.isInstanceOf[AssignedShardEntry]).map(_.shard.asInstanceOf[AssignedShardEntry]).toVector
  }

  /**
   * return the set of all shards which have no associated Address.
   */
  def preparing: Vector[PreparingShardEntry] = {
    entries.filter(_.shard.isInstanceOf[PreparingShardEntry]).map(_.shard.asInstanceOf[PreparingShardEntry]).toVector
  }

  /**
   * return the set of all shards which have no associated Address.
   */
  def migrating: Vector[MigratingShardEntry] = {
    entries.filter(_.shard.isInstanceOf[MigratingShardEntry]).map(_.shard.asInstanceOf[MigratingShardEntry]).toVector
  }

  /**
   * return the set of all shards which have no associated Address.
   */
  def missing: Vector[MissingShardEntry] = {
    entries.filter(_.shard.isInstanceOf[MissingShardEntry]).map(_.shard.asInstanceOf[MissingShardEntry]).toVector
  }

  /**
   * returns true if the ShardMap contains the specified shardId, otherwise false.
   */
  def contains(shardKey: Int): Boolean = apply(shardKey).isDefined

  /**
   * returns the number of shards which have an address (assigned, preparing, or migrating).
   */
  def size: Int = entries.length - numMissing

  def isEmpty: Boolean = numMissing == entries.length

  def nonEmpty: Boolean = !isEmpty

  def isFull: Boolean = numMissing == 0

  def nonFull: Boolean = !isFull

  override def toString = "ShardMap(" + entries.map(_.shard).mkString(",") + ")"
}

object ShardMap {
  def apply(totalShards: Int, initialWidth: Int) = new ShardMap(totalShards, initialWidth)
}

/**
 *
 */
abstract class ShardEntry(_address: Option[Address]) {
  val shardId: Int
  val width: Int
  def contains(shardKey: Int) = shardKey >= shardId && shardKey < shardId + width
  def isEmpty = _address.isEmpty
  def isDefined = _address.isDefined
}

case class AssignedShardEntry(shardId: Int, width: Int, address: Address) extends ShardEntry(Some(address)) {
  override def toString = "<%d+%d assigned to %s>".format(shardId, width, address)
}

case class PreparingShardEntry(shardId: Int, width: Int, address: Address) extends ShardEntry(Some(address)) {
  override def toString = "<%d+%d preparing for %s>".format(shardId, width, address)
}

case class MigratingShardEntry(shardId: Int, width: Int, address: Address) extends ShardEntry(Some(address)) {
  override def toString = "<%d+%d migrating to %s>".format(shardId, width, address)
}

case class MissingShardEntry(shardId: Int, width: Int) extends ShardEntry(None) {
  override def toString = "<%d+%d missing>".format(shardId, width)
}