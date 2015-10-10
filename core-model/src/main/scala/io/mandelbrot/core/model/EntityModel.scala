/**
 * Copyright 2015 Michael Frank <msfrank@syntaxjockey.com>
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

package io.mandelbrot.core.model

import akka.actor.Address
import akka.cluster.MemberStatus

sealed trait EntityModel

/**
 *
 */
abstract class ShardEntry(_address: Option[Address]) extends EntityModel {
  val shardId: Int
  def isEmpty = _address.isEmpty
  def isDefined = _address.isDefined
}

trait DefinedShardEntry extends EntityModel {
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

case class NodeStatus(address: Address, uid: Int, status: MemberStatus, roles: Set[String]) extends EntityModel

case class ClusterStatus(nodes: Vector[NodeStatus], leader: Option[Address], unreachable: Set[Address]) extends EntityModel

case class ShardMapStatus(shards: Set[ShardEntry], totalShards: Int) extends EntityModel
