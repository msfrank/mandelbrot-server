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
import akka.pattern._
import io.mandelbrot.core.{RetryLater, ResourceNotFound, BadRequest, ApiException}
import org.joda.time.DateTime
import scala.concurrent.duration._
import java.util

import io.mandelbrot.core.cluster.EntityFunctions.{ShardResolver, PropsCreator, KeyExtractor}

/**
 * The EntityManager receives EntityEnvelope messages and delivers them to the
 * appropriate shard, which may be local or remote.  The shard may also be in a
 * non-assigned state, in which case the envelope will be buffered until the
 * delivery can be completed.
 *
 * In order to deliver envelopes to the correct destination, the EntityManager
 * must maintain a cached copy of the shard map.  if a StaleShard message is
 * received, then the EntityManager must invalidate the current mapping and query
 * the coordinator for the up-to-date mapping.  The EntityManager also interacts
 * with the ShardBalancer to acquire and release shards as needed to keep shards
 * balanced across the cluster.
 */
class EntityManager(services: ActorRef,
                    shardResolver: ShardResolver,
                    keyExtractor: KeyExtractor,
                    propsCreator: PropsCreator,
                    selfAddress: Address,
                    totalShards: Int,
                    initialWidth: Int) extends Actor with ActorLogging {
  import EntityManager._
  import context.dispatcher

  // config
  val servicesTimeout = 5.seconds
  val taskTimeout = 5.seconds
  val lookupTimeout = 5.seconds

  // state
  val shardMap = ShardMap(totalShards, initialWidth)
  val localEntities = new util.HashMap[Int,ActorRef]()
  val bufferedMessages = new util.ArrayList[EntityEnvelope]()
  val taskTimeouts = new util.HashMap[Int,Cancellable]()
  val staleShards = new util.HashSet[StaleShard]()
  val missingShards = new util.HashMap[Int,ActorRef]

  override def preStart(): Unit = self ! Retry
  
  def initializing: Receive = {

    case Retry =>
      services.ask(ListShards())(servicesTimeout).pipeTo(self)

    case ListShardsResult(op, shards) =>
      // update the shard map and create any local shard entities
      shards.foreach { case Shard(shardId, width, address) =>
        shardMap.assign(shardId, address)
        // if shard is local and entity map doesn't exist, create a new entity map
        if (address.equals(selfAddress)) {
          val shardEntities = context.actorOf(ShardEntities.props(services, keyExtractor, propsCreator))
          localEntities.put(shardId, shardEntities)
        }
      }
      log.debug("initialized shard map with {} shards, {} missing", shardMap.size, shardMap.numMissing)
      // start lookup tasks for any missing shards
      shardMap.missing.foreach { case entry: MissingShardEntry =>
        val op = LookupShard(entry.shardId)
        val actor = context.actorOf(LookupShardTask.props(op, services, self, totalShards, initialWidth, lookupTimeout))
        missingShards.put(entry.shardId, actor)
      }
      context.become(running)
      // flush any envelopes which were received while initializing
      flushBuffered()

    case failure: ClusterServiceOperationFailed =>
      log.debug("failed to list shards: {}", failure.failure.getCause)
      context.system.scheduler.scheduleOnce(servicesTimeout, self, Retry)

    case failure: AskTimeoutException =>
      log.debug("failed to list shards: timed out")
      context.system.scheduler.scheduleOnce(servicesTimeout, self, Retry)

    case op: ShardBalancerOperation =>
      sender() ! ShardBalancerOperationFailed(op, new ApiException(RetryLater))

    case envelope: EntityEnvelope =>
      deliverEnvelope(envelope)
  }
  
  def receive = initializing
  
  def running: Receive = {

    // send the specified message to the entity, which may be remote or local
    case envelope: EntityEnvelope =>
      deliverEnvelope(envelope)

    // start buffering messages in preparation to own shard
    case op: PrepareShard =>
      shardMap.get(op.shardId) match {
        case entry: AssignedShardEntry =>
          sender() ! ShardBalancerOperationFailed(op, new ApiException(BadRequest))
        case entry: ShardEntry if taskTimeouts.containsKey(op.shardId) =>
          sender() ! ShardBalancerOperationFailed(op, new ApiException(RetryLater))
        case entry: ShardEntry =>
          log.debug("{} says prepare shardId {}", sender().path, op.shardId)
          shardMap.prepare(op.shardId, selfAddress)
          taskTimeouts.put(op.shardId, context.system.scheduler.scheduleOnce(taskTimeout, self, TaskTimeout(op.shardId)))
          sender() ! PrepareShardResult(op)
      }

    // change preparing shard to assigned
    case op: RecoverShard =>
      shardMap.get(op.shardId) match {
        case entry: PreparingShardEntry if !taskTimeouts.containsKey(op.shardId) =>
          sender() ! ShardBalancerOperationFailed(op, new ApiException(RetryLater))
        case entry: PreparingShardEntry =>
          log.debug("{} says recover shardId {}", sender().path, op.shardId)
          shardMap.assign(op.shardId, selfAddress)
          val shardEntities = context.actorOf(ShardEntities.props(services, keyExtractor, propsCreator))
          localEntities.put(op.shardId, shardEntities)
          taskTimeouts.remove(op.shardId).cancel()
          sender() ! RecoverShardResult(op)
        case entry: ShardEntry =>
          sender() ! ShardBalancerOperationFailed(op, new ApiException(BadRequest))
      }

    // the Put or Migrate task did not complete
    case TaskTimeout(shardId) =>
      shardMap.remove(shardId)
      taskTimeouts.remove(shardId)
      if (!missingShards.containsKey(shardId)) {
        val op = LookupShard(shardId)
        val actor = context.actorOf(LookupShardTask.props(op, services, self, totalShards, initialWidth, lookupTimeout))
        missingShards.put(shardId, actor)
      }

    // update shard map and flush buffered messages
    case result: LookupShardResult =>
      missingShards.remove(result.shardId)
      if (!taskTimeouts.containsKey(result.shardId)) {
        log.debug("assigning shard {} to {}", result.shardId, result.address)
        shardMap.assign(result.shardId, result.address)
        // if shard is local and entity map doesn't exist, create a new entity map
        if (result.address.equals(selfAddress) && !localEntities.containsKey(result.shardId)) {
          val shardEntities = context.actorOf(ShardEntities.props(services, keyExtractor, propsCreator))
          localEntities.put(result.shardId, shardEntities)
        }
        // flush any buffered messages for newly assigned shards
        flushBuffered()
      }

    // cluster service operation failed
    case failure: ClusterServiceOperationFailed =>
      log.debug("operation {} failed: {}", failure.op, failure.failure)

    // a shard terminated
    case Terminated(ref) =>
      // FIXME
  }

  /**
   *
   */
  def deliverEnvelope(envelope: EntityEnvelope): Unit = if (keyExtractor.isDefinedAt(envelope.message)) {
    val shardKey = shardResolver(envelope.message)
    shardMap(shardKey) match {

      // shard is assigned to this node
      case shard: AssignedShardEntry if shard.address.equals(selfAddress) =>
        localEntities.get(shard.shardId) match {
          // entity doesn't exist in shard
          case null =>
            envelope.sender ! EntityDeliveryFailed(envelope, new ApiException(RetryLater))
          // entity exists, forward the message to it
          case entity: ActorRef =>
            entity forward envelope
        }

      // shard is assigned to a remote node
      case shard: AssignedShardEntry =>
        if (envelope.attempts > 0) {
          val selection = context.system.actorSelection(RootActorPath(shard.address) / self.path.elements)
          selection ! envelope.copy(attempts = envelope.attempts - 1)
          // if the sender is remote, then note the stale shard mapping
          if (!envelope.sender.path.address.equals(selfAddress))
            staleShards.add(StaleShard(shard.shardId, shard.address))
        } else envelope.sender ! EntityDeliveryFailed(envelope, new ApiException(ResourceNotFound))

      // shard is in the process of being assigned to this node
      case shard: PreparingShardEntry =>
        log.debug("shard {} is preparing, buffering {}", shard.shardId, envelope.message)
        bufferedMessages.add(envelope)

      // shard is in the process of being migrated from this node
      case shard: MigratingShardEntry =>
        if (envelope.attempts > 0) {
          val selection = context.system.actorSelection(RootActorPath(shard.address) / self.path.elements)
          selection ! envelope.copy(attempts = envelope.attempts - 1)
          // if the sender is remote, then note the stale shard mapping
          if (!envelope.sender.path.address.equals(selfAddress))
            staleShards.add(StaleShard(shard.shardId, shard.address))
        } else envelope.sender ! EntityDeliveryFailed(envelope, new ApiException(ResourceNotFound))

      // shard is missing from the shard map
      case shard: MissingShardEntry =>
        log.debug("shard not known for entity {}, buffering {}", shardKey, envelope.message)
        bufferedMessages.add(envelope)
        // if we are not performing a task on the shard...
        if (!taskTimeouts.containsKey(shard.shardId)) {
          // and we are not already trying to look up the shard...
          if (!missingShards.containsKey(shard.shardId)) {
            /// then try to repair the shard map
            val op = LookupShard(shard.shardId)
            val actor = context.actorOf(LookupShardTask.props(op, services, self, totalShards, initialWidth, lookupTimeout))
            missingShards.put(shard.shardId, actor)
          }
        }
    }
  } else envelope.sender ! EntityDeliveryFailed(envelope, new ApiException(BadRequest))

  /**
   *
   */
  def flushBuffered(): Unit = {
    val iterator = bufferedMessages.listIterator()
    while (iterator.hasNext) {
      val envelope = iterator.next()
      shardMap(shardResolver(envelope.message)) match {
        case entry: AssignedShardEntry =>
          iterator.remove()
          deliverEnvelope(envelope)
        case entry => // do nothing
      }
    }
  }
}

object EntityManager {
  def props(services: ActorRef,
            shardResolver: ShardResolver,
            keyExtractor: KeyExtractor,
            propsCreator: PropsCreator,
            selfAddress: Address,
            totalShards: Int,
            initialWidth: Int) = {
    Props(classOf[EntityManager], services, shardResolver, keyExtractor, propsCreator, selfAddress, totalShards, initialWidth)
  }

  case object Retry
  case class StaleShard(shardId: Int, address: Address)
  case class TaskTimeout(shardId: Int)
}

object EntityFunctions {
  type KeyExtractor = PartialFunction[Any,String]
  type ShardResolver = PartialFunction[Any,Int]
  type PropsCreator = PartialFunction[Any,Props]
}

case class EntityEnvelope(sender: ActorRef, message: Any, attempts: Int)
case class EntityDeliveryFailed(envelope: EntityEnvelope, failure: Throwable)

