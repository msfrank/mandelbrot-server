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

package io.mandelbrot.core.cluster

import akka.actor._
import scala.concurrent.duration._
import scala.collection.mutable

import ShardBalancer.{State, Data}

/**
 * The ShardBalancer is responsible for keeping the shard map healthy.  this consists
 * of multiple interrelated tasks:
 *   1) ensure that all shards are assigned to an address.
 *   2) ensure that shards are equally balanced across the cluster when members join the cluster.
 *   3) ensure that shards are equally balanced across the cluster when members leave the cluster.
 *   4) ensure that shards are redistributed when members get too hot or cold.
 */
class ShardBalancer(coordinator: ActorRef, monitor: ActorRef, nodes: Map[Address,ActorPath], totalShards: Int, initialWidth: Int) extends LoggingFSM[State,Data] {
  import ShardBalancer._

  // config
  val timeout = 5.seconds

  // state
  val shardMap = ShardMap(totalShards, initialWidth)
  val shardDensity = new mutable.HashMap[Address,Int]()
  var missingShards = Map.empty[Int,MissingShardEntry]
  var addedNodes = Set.empty[Address]
  var removedNodes = Set.empty[Address]

  override def preStart(): Unit = {
    // get the current location of all shards
    coordinator ! GetAllShards()
  }

  startWith(Initializing, NoData)

  when(Initializing) {

    // gather the current state of all shards
    case Event(result: GetAllShardsResult, NoData) =>

      // build shardMap and shardDensity from coordinator data
      result.shards.foreach { case Shard(shardId, width, address) =>
        shardMap.assign(shardId, address)
        val numShards = shardDensity.getOrElse(address, 0)
        shardDensity.put(address, numShards + 1)
      }
      // find shards which are not mapped to any address
      missingShards = shardMap.missing.map(missing => missing.shardId -> missing).toMap
      // find nodes which have been added since the last balancing and add to densities map
      addedNodes = nodes.keySet diff shardDensity.keySet
      addedNodes.foreach(shardDensity.put(_, 0))
      // find nodes which have been added since the last balancing
      // TODO: handle rebalancing for node removals
      removedNodes = shardDensity.keySet.toSet diff nodes.keySet
      log.debug("shard assignments {}", shardMap)
      log.debug("missing shards {}", missingShards)
      log.debug("shard densities {}", shardDensity)
      log.debug("added nodes {}", addedNodes)
      log.debug("removed nodes {}", removedNodes)
      // transition to the next State
      balance()
  }

  when(Repairing) {

    // update state and process the next operation, if any
    case Event(result: PutShardComplete, state: Repairing) =>
      val PutShard(shardId, actorPath) = result.op
      val address = actorPath.address
      shardMap.assign(shardId, address)
      val numShards = shardDensity.getOrElse(address, 0)
      shardDensity.put(address, numShards + 1)
      missingShards = missingShards - shardId
      if (state.queued.nonEmpty) {
        val inflight = context.actorOf(PutShardTask.props(state.queued.head, coordinator, self, timeout))
        stay() using Repairing(inflight, state.queued.tail)
      } else balance()

    // the operation failed for some reason
    case Event(result: PutShardFailed, state: Repairing) =>
      val PutShard(shardId, actorPath) = result.op
      log.debug("failed to put shard {} at {}: {}", shardId, actorPath.address, result.ex)
      if (state.queued.nonEmpty) {
        val inflight = context.actorOf(PutShardTask.props(state.queued.head, coordinator, self, timeout))
        stay() using Repairing(inflight, state.queued.tail)
      } else balance()
  }

  /**
   * decide the appropriate State to transition to based on the current state.
   */
  def balance(): State = {
    if (missingShards.nonEmpty) {
      // sort addresses by density
      val addressesSortedByDensity = mutable.PriorityQueue()(ordering)
      shardDensity.foreach(addressesSortedByDensity.enqueue(_))
      // create PutShard operations for any missing shards
      val ops = shardMap.missing.map { missingShard =>
        val (address,numShards) = addressesSortedByDensity.dequeue()
        addressesSortedByDensity.enqueue((address, numShards + 1))
        PutShard(missingShard.shardId, nodes(address))
      }.toVector
      // put the first operation in flight
      val inflight = context.actorOf(PutShardTask.props(ops.head, coordinator, self, timeout))
      goto(Repairing) using Repairing(inflight, ops.tail)
    } else {
      monitor ! ShardBalancerResult(shardMap)
      stop()
    }
  }

  /**
   * calculate the standard deviation from the specified iterable of densities.
   */
  def calculateDensityDeviation(densities: Iterable[Int]): Double = {
    val cardinality = densities.size
    val mean = densities.reduce(_ + _) / cardinality
    val variance = densities.foldLeft(0.toDouble) { case (acc,n) => math.pow(n - mean, 2) + acc } / cardinality
    math.sqrt(variance)
  }

  // must be at the end of the constructor
  initialize()
}

object ShardBalancer {
  def props(coordinator: ActorRef, monitor: ActorRef, nodes: Map[Address,ActorPath], totalShards: Int, initialWidth: Int) = {
    Props(classOf[ShardBalancer], coordinator, monitor, nodes, totalShards, initialWidth)
  }

  val ordering = Ordering.by[(Address,Int),Int](_._2).reverse

  case class MigrateShard(shardId: Int, targetNode: Address, sourceNode: Option[ActorPath])

  sealed trait State
  case object Initializing extends State
  case object Migrating extends State
  case object Repairing extends State

  sealed trait Data
  case object NoData extends Data
  case class Migrating(inflight: ActorRef, queued: Vector[MoveShard]) extends Data
  case class Repairing(inflight: ActorRef, queued: Vector[PutShard]) extends Data
}

case class ShardBalancerResult(shardMap: ShardMap)