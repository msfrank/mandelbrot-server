/**
 * Copyright 2014 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Mandelbrot.
 *
 * Mandelbrot is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Mandelbrot is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Mandelbrot.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mandelbrot.core.entity

import akka.actor.Address

/**
 * The ShardMap contains the current mapping of each shard to and address.  Each
 * shard can be in one of four states:
 *   1) assigned: the shard is currently assigned to the specified address.
 *   2) preparing: the shard is going to be assigned to the specified address.
 *   3) migrating: the shard is moving to another address.
 *   4) missing: the shard has no address, or the address is not known.
 */
class ShardMap(totalShards: Int) {

  // FIXME: assert invariants

  private class MapEntry(_shard: ShardEntry) {
    var shard: ShardEntry = _shard
    var left: Option[MapEntry] = None
    var right: Option[MapEntry] = None
  }

  // initialize the shard map with missing entries
  private val entries = new Array[MapEntry](totalShards)
  entries.indices.foreach { shardId =>
    entries(shardId) = new MapEntry(MissingShardEntry(shardId))
  }

  private var _numMissing = entries.length

  /**
   * get the shard entry for the specified shardId.
   */
  private def getShardEntry(shardId: Int): MapEntry = {
    if (shardId < 0 || shardId >= totalShards)
      throw new IllegalArgumentException("shardId %d is out of range".format(shardId))
    entries(shardId)
  }

  /**
   * get the shard for the specified shardId.
   */
  def get(shardId: Int): ShardEntry = getShardEntry(shardId).shard

  /**
   * convert the specified shardKey into a shardId.
   */
  def getShardId(shardKey: Int): Int = scala.math.abs(shardKey) % totalShards

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
    entry.shard = AssignedShardEntry(prev.shardId, address)
    if (prev.isEmpty)
      _numMissing = _numMissing - 1
    prev
  }

  /**
   * assign an address to the specified shard, returning the previous shard.
   */
  def prepare(shardId: Int, address: Address): ShardEntry = {
    val entry = getShardEntry(shardId)
    val prev = entry.shard
    entry.shard = PreparingShardEntry(prev.shardId, address)
    if (prev.isEmpty)
      _numMissing = _numMissing - 1
    prev
  }

  /**
   * assign an address to the specified shard, returning the previous shard.
   */
  def migrate(shardId: Int, address: Address): ShardEntry = {
    val entry = getShardEntry(shardId)
    val prev = entry.shard
    entry.shard = MigratingShardEntry(prev.shardId, address)
    if (prev.isEmpty)
      _numMissing = _numMissing - 1
    prev
  }

  /**
   * remove the specified shard from the ShardMap, returning the previous shard.
   */
  def remove(shardId: Int): ShardEntry = {
    val entry = getShardEntry(shardId)
    val prev = entry.shard
    entry.shard = MissingShardEntry(prev.shardId)
    if (prev.isDefined)
      _numMissing = _numMissing + 1
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
   * return the set of all shards which are preparing to move to a different Address.
   */
  def preparing: Vector[PreparingShardEntry] = {
    entries.filter(_.shard.isInstanceOf[PreparingShardEntry]).map(_.shard.asInstanceOf[PreparingShardEntry]).toVector
  }

  /**
   * return the set of all shards which are moving to a different Address.
   */
  def migrating: Vector[MigratingShardEntry] = {
    entries.filter(_.shard.isInstanceOf[MigratingShardEntry]).map(_.shard.asInstanceOf[MigratingShardEntry]).toVector
  }

  /**
   * return the set of all shards which have a current address.
   */
  def defined: Vector[DefinedShardEntry] = {
    entries.filter(_.shard.isInstanceOf[DefinedShardEntry]).map(_.shard.asInstanceOf[DefinedShardEntry]).toVector
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
   * returns the total number of shards.
   */
  def size: Int = entries.length

  /**
   * returns the number of shards which are missing.
   */
  def numMissing: Int = _numMissing

  /**
   * returns the number of shards which have an address (assigned, preparing, or migrating).
   */
  def numDefined: Int = entries.length - _numMissing

  def isEmpty: Boolean = _numMissing == entries.length

  def nonEmpty: Boolean = !isEmpty

  def isFull: Boolean = _numMissing == 0

  def nonFull: Boolean = !isFull

  override def toString = "ShardMap(" + entries.map(_.shard).mkString(",") + ")"
}

object ShardMap {
  def apply(totalShards: Int) = new ShardMap(totalShards)
}

/**
 *
 */
abstract class ShardEntry(_address: Option[Address]) {
  val shardId: Int
  def isEmpty = _address.isEmpty
  def isDefined = _address.isDefined
}

trait DefinedShardEntry {
  val shardId: Int
  val address: Address
}

case class AssignedShardEntry(shardId: Int, address: Address) extends ShardEntry(Some(address)) with DefinedShardEntry {
  override def toString = "Shard(%d assigned to %s)".format(shardId, address)
}

case class PreparingShardEntry(shardId: Int, address: Address) extends ShardEntry(Some(address)) with DefinedShardEntry {
  override def toString = "Shard(%d preparing for %s)".format(shardId, address)
}

case class MigratingShardEntry(shardId: Int, address: Address) extends ShardEntry(Some(address)) with DefinedShardEntry {
  override def toString = "Shard(%d migrating to %s)".format(shardId, address)
}

case class MissingShardEntry(shardId: Int) extends ShardEntry(None) {
  override def toString = "Shard(%d missing)".format(shardId)
}